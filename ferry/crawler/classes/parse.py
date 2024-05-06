import re
import warnings
from pathlib import Path
from tqdm import tqdm
from typing import cast, Any, TypedDict

import ujson
from unidecode import unidecode
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning, ResultSet, Tag

from ferry.utils import convert_unicode
from ferry.crawler.cache import load_cache_json, save_cache_json

warnings.filterwarnings("ignore", category=MarkupResemblesLocatorWarning, module="bs4")

PROFESSOR_EXCEPTIONS = {
    "Kimberly Shirkhani": "Kim Shirkhani",
    "John Green": "Derek Green",
}

COLLEGE_SEMINAR_CODES = {
    "CSBF",  # Coll Sem: Ben Franklin Coll
    "CSBK",  # Coll Sem: Berkeley Coll
    "CSBR",  # Coll Sem: Branford Coll
    "CSDC",  # Coll Sem: Davenport Coll
    "CSES",  # Coll Sem: Ezra Stiles Coll
    "CSGH",  # Coll Sem: Grace Hopper Coll
    "CSJE",  # Coll Sem: Jonathan Edwards Coll
    "CSMC",  # Coll Sem: Morse Coll
    "CSMY",  # Coll Sem: Pauli Murray Coll
    "CSPC",  # Coll Sem: Pierson Coll
    "CSSM",  # Coll Sem: Silliman Coll
    "CSSY",  # Coll Sem: Saybrook Coll
    "CSTC",  # Coll Sem: Trumbull Coll
    "CSTD",  # Coll Sem: Timothy Dwight Coll
    "CSYC",  # Coll Sem: Yale Coll
}


class ParsedProfessors(TypedDict):
    professors: list[str]
    professor_emails: list[str]
    professor_ids: list[str]


def extract_professors(html: str) -> ParsedProfessors:
    soup = BeautifulSoup(html, features="lxml")
    instructor_divs = cast(
        ResultSet[Tag], soup.findAll("div", {"class": "instructor-detail"})
    )

    names = []
    emails = []
    ids = []  # Yale course search's internal professor ID

    for div in instructor_divs:
        instructor_name = div.find("div", {"class": "instructor-name"})
        instructor_email = div.find("div", {"class": "instructor-email"})
        instructor_id = ""  # default

        if type(instructor_name) == Tag:
            # check if the professor has an associated ID
            instructor_search = instructor_name.find("a", {"data-action": "search"})
            if type(instructor_search) == Tag:
                instructor_id = str(instructor_search["data-id"])
            instructor_name = instructor_name.get_text()
        else:
            instructor_name = ""

        if instructor_email:
            instructor_email = instructor_email.get_text()
        else:
            instructor_email = ""

        # remove accents from professor names
        instructor_name = unidecode(instructor_name)

        # patch certain professor names manually
        instructor_name = PROFESSOR_EXCEPTIONS.get(instructor_name, instructor_name)

        # if the professor has a name and is not listed as staff, add it
        if len(instructor_name) > 0 and instructor_name != "Staff":
            names.append(instructor_name)
            emails.append(instructor_email)
            ids.append(instructor_id)

    # skip sorting and return empty
    if len(names) == 0:
        return {"professors": [], "professor_emails": [], "professor_ids": []}

    # parallel sort by instructor name
    names, emails, ids = zip(*sorted(zip(names, emails, ids)))

    return {"professors": names, "professor_emails": emails, "professor_ids": ids}


def parse_cross_listings(xlist_html: str) -> list[str]:
    """
    Retrieve cross-listings (CRN codes) from the HTML in the 'xlist' field from the Yale API.

    Note that the cross-listings do not include the course itself.

    Parameters
    ----------
    xlist_html:
        'xlist' field from the Yale API response.

    Returns
    -------
    xlist_crns:
        CRN codes of course cross-listings.
    """
    xlist_soup = BeautifulSoup(xlist_html, features="lxml")

    xlist_crns = xlist_soup.find_all("a", {"data-action": "result-detail"})
    xlist_crns = [x["data-key"] for x in xlist_crns]
    xlist_crns = [x[4:] for x in xlist_crns if x[:4] == "crn:"]

    return xlist_crns


def extract_credits(credit_html: str, hours: str) -> float:
    try:
        if hours != "":
            return float(hours)
        if credit_html.endswith(
            " credit for Yale College students"
        ) or credit_html.endswith(" credits for Yale College students"):
            return float(credit_html[:-33])
    except ValueError:
        pass
    # non-Yale College courses don't have listed credits, so assume they are 1
    return 1.0


def extract_flags(ci_attrs: str) -> list[str]:
    """
    Get the course flags from the ci_attrs field.

    Parameters
    ----------
    ci_attrs:
        the field from the Yale API response.

    Returns
    -------
    flag_texts:
        Flags (keywords) for the course.
    """
    soup = BeautifulSoup(ci_attrs, features="lxml")
    flag_texts = [x.get_text() for x in cast(ResultSet[Tag], soup.find_all("a"))]

    return flag_texts


def days_of_week_from_letters(letters: str) -> list[str]:
    """
    Parse course days from letterings.

    Parameters
    ----------
    letters:
        Course meeting days, abbreviated.

    Returns
    -------
    days:
        Days on which course meets.
    """
    if letters == "M-F":
        return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    days: list[str] = []

    letter_to_day = {
        "M": "Monday",
        "T(?!h)": "Tuesday",  # avoid misidentification as Thursday
        "W": "Wednesday",
        "Th": "Thursday",
        "F": "Friday",
        "Sa": "Saturday",
        "Su": "Sunday",
    }

    for letter, day in letter_to_day.items():
        if re.search(letter, letters):
            days.append(day)

    return days


def format_time(time: str) -> str:
    """
    Convert Yale API times to 24-hour, full format.

    Parameters
    ----------
    time:
        A time from the Yale API 'meeting_html' field.

    Returns
    -------
    time:
        formatted time
    """
    time_stripped = time.replace(";", "")[:-2]

    if ":" in time_stripped:
        hour, minute = time_stripped.split(":")[:2]
    else:
        hour, minute = time_stripped, "00"

    hour_num = int(hour)

    if time[-2:] == "am" and hour_num == 12:
        hour_num = 0

    elif time[-2:] == "pm" and 1 <= hour_num <= 11:
        hour_num = hour_num + 12

    hour = str(hour_num)

    return f"{hour}:{minute}"


def split_meeting_text(meeting_text: str) -> tuple[str, str, str]:
    """
    Split meeting string into a [days, time, location] tuple.
    """
    if " in " in meeting_text:
        sessions, location = meeting_text.split(" in ")[:2]
        if " " in sessions:
            days, time = sessions.split(" ")[:2]
            return days, time, location
        else:
            return "HTBA", "", location
    elif " " in meeting_text:
        days, time = meeting_text.split(" ")[:2]
        return days, time, ""
    else:
        return meeting_text, "", ""


def create_meeting_summaries(
    split_meetings: list[tuple[str, str, str]]
) -> tuple[str, str]:
    """
    Get meeting time and location summary strings.

    Parameters
    ----------
    split_meetings:
        split meetings from split_meeting_text. list of meeting [days, time, location]

    Returns
    -------
    times_summary, locations_summary
    """
    # make times and locations summary as first listed
    times_summary = f"{split_meetings[0][0]} {split_meetings[0][1]}"
    locations_summary = split_meetings[0][2]

    # collapse additional times/locations
    if len(split_meetings) > 1:
        times_summary += f" + {len(split_meetings)-1}"
        locations_summary += f" + {len(split_meetings)-1}"

    # some final touches
    times_summary = times_summary.replace("MTWThF", "M-F")
    locations_summary = locations_summary.replace("MTWThF", "M-F")

    if locations_summary == "" or locations_summary.startswith(" + "):
        locations_summary = "TBA"

    # handle redundant dash-delimited format (introduced in fall 2020)
    if locations_summary.count(" - ") == 1:
        locations_1, locations_2 = locations_summary.split(" - ")

        # if location is redundant
        if locations_2.startswith(locations_1):
            locations_summary = locations_2

    return times_summary, locations_summary


def create_times_by_day(
    split_meetings: list[tuple[str, str, str]], location_urls: list[str]
) -> dict[str, list[tuple[str, str, str, str]]]:
    """
    Transform formatted meetings.

    Parameters
    ----------
    split_meetings:
        split meetings from split_meeting_text. list of meeting [days, time, location]
    location_urls:
        list of location urls

    Returns
    ---
        {
            day: [
                [start_time, end_time, location, location_url]
                ...
            ]
        }
    """
    times_by_day: dict[str, list[tuple[str, str, str, str]]] = {}

    for i, (days, times, location) in enumerate(split_meetings):
        if days == "HTBA":
            continue
        days_split = days_of_week_from_letters(days)

        times_split = times.split("-")
        start_time = times_split[0]
        end_time = times_split[1]

        # standardize times to 24-hour, full format
        start_time = format_time(start_time)
        end_time = format_time(end_time)
        for day in days_split:
            session = start_time, end_time, location, location_urls[i]
            if day in times_by_day:
                times_by_day[day].append(session)
            else:
                times_by_day[day] = [session]

    return times_by_day


class ParsedMeeting(TypedDict):
    times_summary: str
    locations_summary: str
    times_by_day: dict[str, list[tuple[str, str, str, str]]]


def extract_meetings(
    meeting_html: str,
    all_in_group: list[dict[str, Any]],
) -> ParsedMeeting:
    """
    Extract course meeting times and locations from the provided HTML.

    Parameters
    ----------
    meeting_html:
        HTML of course meeting times, specified by the 'meeting_html' key in the Yale API response.

    Returns
    -------
    times_summary: string
        Summary of meeting times; the first listed times are shown while additional ones are
        collapsed to " + (n-1)".
    locations_summary:
        Summary of meeting locations; the first listed ocations are shown while additional ones
        are collapsed to " + (n-1)".
    times_by_day:
        Dictionary with keys as days and values consisting of lists of
        [start_time, end_time, location, location_url]
    """
    if meeting_html == "":
        return extract_meetings_alternate(all_in_group)

    # identify meeting tags and convert to plaintext
    meeting_entries = BeautifulSoup(meeting_html, features="lxml").find_all(
        "div", {"class": "meet"}
    )
    if len(meeting_entries) == 0 or meeting_entries[0].text == "HTBA":
        return {
            "times_summary": "TBA",
            "locations_summary": "TBA",
            "times_by_day": {},
        }

    location_urls: list[str] = []
    meetings: list[str] = []
    for meeting in meeting_entries:
        link = meeting.find("a")
        if link:
            location_urls.append(link["href"])
        else:
            location_urls.append("")
        meetings.append(meeting.text)

    split_meetings = list(map(split_meeting_text, meetings))
    times_summary, locations_summary = create_meeting_summaries(split_meetings)

    return {
        "times_summary": times_summary,
        "locations_summary": locations_summary,
        "times_by_day": create_times_by_day(split_meetings, location_urls),
    }


def format_undelimited_time(time: str) -> str:
    """
    Convert an undelimited time string (e.g. "1430") to a delimited one (e.g. "14:30").

    Parameters
    ----------
    time:
        input time
    """
    hours, minutes = time[:-2], time[-2:]

    return f"{hours}:{minutes}"


DAYS_MAP = {
    "0": "Monday",
    "1": "Tuesday",
    "2": "Wednesday",
    "3": "Thursday",
    "4": "Friday",
    "5": "Saturday",
    "6": "Sunday",
}


def extract_meetings_alternate(all_in_group: list[dict[str, Any]]) -> ParsedMeeting:
    """
    Extract course meeting times from the allInGroup key rather than meeting_html. Note that this
    does not return locations because they are not specified.

    Parameters
    ----------
    course_json:
        course_json object read from course_json_cache.

    Returns
    -------
    times_summary:
        Summary of meeting times; the first listed times are shown while additional ones are
        collapsed to " + (n-1)"
    locations_summary:
        Summary of meeting locations; the first listed locations are shown while additional
        ones are collapsed to " + (n-1)"
    times_by_day:
        Dictionary with keys as days and values consisting of lists of
        [start_time, end_time, location]
    """
    if len(all_in_group) == 0:
        return {
            "times_summary": "TBA",
            "locations_summary": "TBA",
            "times_by_day": {},
        }

    # use the first listing (for when a course has multiple)
    primary_listing = all_in_group[0]
    times_summary = primary_listing["meets"]
    if times_summary == "HTBA":
        times_summary = "TBA"
    meeting_times = ujson.loads(primary_listing["meetingTimes"])
    times_by_day: dict[str, list[tuple[str, str, str, str]]] = {}
    for meeting_time in meeting_times:
        meeting_day = DAYS_MAP[meeting_time["meet_day"]]
        session = (
            format_undelimited_time(meeting_time["start_time"]),
            format_undelimited_time(meeting_time["end_time"]),
            "",
            "",
        )
        if meeting_day in times_by_day:
            times_by_day[meeting_day].append(session)
        else:
            times_by_day[meeting_day] = [session]

    return {
        "times_summary": times_summary,
        "locations_summary": "TBA",
        "times_by_day": times_by_day,
    }


def extract_resource_link(resource_html: str, title: str) -> str:
    matched_link = re.findall(f'href="([^"]*)"[^>]*>{title}</a>', resource_html)

    if len(matched_link) > 0:
        return matched_link[0]
    else:
        return ""


# abbreviations for skills
SKILLS_MAP = {
    "Writing": "WR",
    "Quantitative Reasoning": "QR",
    "Language (1)": "L1",
    "Language (2)": "L2",
    "Language (3)": "L3",
    "Language (4)": "L4",
    "Language (5)": "L5",
}

# abbreviations for areas
AREAS_MAP = {
    "Humanities": "Hu",
    "Social Sciences": "So",
    ">Sciences": "Sc",
}

# abbreviations for course statuses
STAT_MAP = {
    "A": "ACTIVE",
    "B": "MOVED_TO_SPRING_TERM",
    "C": "CANCELLED",
    "D": "MOVED_TO_FALL_TERM",
    "E": "CLOSED",
    "N": "NUMBER_CHANGED",
}


def extract_skills_areas(text: str, codes_map: dict[str, str]) -> list[str]:
    codes: list[str] = []

    for search_text, code in codes_map.items():
        if search_text in text:
            codes.append(code)

    return sorted(codes)


class ParsedDescription(TypedDict):
    requirements: str
    description: str


def extract_prereqs_and_description(description_html: str) -> ParsedDescription:
    raw_description = BeautifulSoup(convert_unicode(description_html), features="lxml")

    # course prerequisites
    prereq_elems = raw_description.findAll("p", {"class": "prerequisites"})
    requirements = "\n".join([x.get_text() for x in prereq_elems])

    # remove prereqs from the description
    for div in prereq_elems:
        div.decompose()

    description_text = raw_description.get_text().rstrip()

    return {"requirements": requirements, "description": description_text}


def extract_note(raw_html: str) -> str:
    return BeautifulSoup(raw_html, features="lxml").get_text().replace("  ", " ")


def is_fysem(code: str, description_text: str, requirements_text: str) -> bool:
    # directed studies courses are basically first-year seminars
    if code.startswith("DRST 0"):
        return True
    flagged_text = [
        "Freshman Seminar Program",
        "First-Year Seminar Program",
        "Enrollment limited to freshmen",
        "Enrollment limited to first-years",
        "Enrollment limited to first-year students",
        "Intended for freshmen",
        "Intended for first-year students",
        "Priority to freshmen",
        "Priority to first-years",
        "preference to freshmen",
        "preference to first-years",
        "primarily for freshmen",
        "primarily for first-years",
    ]
    return any(
        text in description_text or text in requirements_text for text in flagged_text
    )


def is_sysem(title_text: str, description_text: str, requirements_text: str) -> bool:
    flagged_text = [
        "Enrollment limited to sophomores",
        "Sophomore Seminar",
        "Registration preference to sophomores",
        "Registration preference given to sophomores",
        "Registration preference is given to sophomores",
        "Enrollment limited to freshmen and sophomores",
        "Enrollment limited to first-years and sophomores",
        "Enrollment limited to sophomores",
        "Preference to sophomores",
        "Sophomore standing required",
        "Recommended for sophomores",
        "Intended for freshmen and sophomores",
        "Intended for first-year students and sophomores",
        "Priority to sophomores",
        "preference to freshmen and sophomores",
        "preference to first-years and sophomores",
        "primarily for freshmen and sophomores",
        "primarily for first-years and sophomores",
    ]
    return any(
        text in title_text or text in description_text or text in requirements_text
        for text in flagged_text
    )


class ParsedCourse(TypedDict):
    season_code: str
    requirements: str
    description: str
    title: str
    school: str
    credits: float
    extra_info: str
    professors: list[str]
    professor_emails: list[str]
    professor_ids: list[str]
    crn: str
    crns: list[str]
    course_code: str
    subject: str
    number: str
    section: str
    times_summary: str
    locations_summary: str
    times_by_day: dict[str, list[tuple[str, str, str, str]]]
    skills: list[str]
    areas: list[str]
    flags: list[str]
    regnotes: str
    rp_attr: str
    classnotes: str
    final_exam: str
    course_home_url: str
    syllabus_url: str
    fysem: bool
    sysem: bool
    colsem: bool


def extract_course_info(
    course_json: dict[str, Any], season: str, fysem: set[str]
) -> ParsedCourse:
    """
    Parse the JSON response from the Yale courses API into a more useful format.

    Parameters
    ----------
    course_json:
        JSON response from courses API.
    season:
        The season that the courses belong to (required because not returned by the Yale API).
    fysem:
        CRNs of first-year seminars.

    Returns
    -------
    course_info:
        Processed course information
    """
    # The order of keys is significant (impacts JSON diff)! Add them all at once
    # to avoid changing them
    course_info: ParsedCourse = {
        "season_code": season,
        **extract_prereqs_and_description(course_json["description"]),
        "title": course_json["title"],
        "school": course_json["col"],
        "credits": extract_credits(
            course_json.get("credit_html", ""), course_json.get("hours", "")
        ),
        "extra_info": STAT_MAP.get(course_json["stat"], "ACTIVE"),
        **extract_professors(convert_unicode(course_json["instructordetail_html"])),
        "crn": course_json["crn"],
        "crns": [course_json["crn"], *parse_cross_listings(course_json["xlist"])],
        "course_code": course_json["code"],
        "subject": course_json["code"].split(" ")[0],
        "number": course_json["code"].split(" ")[1],
        "section": course_json["section"].lstrip("0"),
        **extract_meetings(
            course_json.get("meeting_html", ""), course_json.get("allInGroup", [])
        ),
        "skills": extract_skills_areas(course_json["yc_attrs"], SKILLS_MAP),
        "areas": extract_skills_areas(course_json["yc_attrs"], AREAS_MAP),
        "flags": extract_flags(course_json["ci_attrs"]),
        "regnotes": extract_note(course_json["regnotes"]),
        "rp_attr": extract_note(course_json["rp_attr"]),
        "classnotes": extract_note(course_json.get("clssnotes", "")),
        "final_exam": course_json["final_exam"],
        "course_home_url": extract_resource_link(course_json["resources"], "HOMEPAGE"),
        "syllabus_url": extract_resource_link(course_json["resources"], "SYLLABUS"),
        "fysem": False,
        "sysem": False,
        "colsem": False,
    }

    course_info["fysem"] = course_json["crn"] in fysem or is_fysem(
        course_json["code"],
        course_info["description"],
        course_info["requirements"],
    )

    course_info["sysem"] = is_sysem(
        course_info["title"],
        course_info["description"],
        course_info["requirements"],
    )

    course_info["colsem"] = course_info["subject"] in COLLEGE_SEMINAR_CODES

    return course_info


# combine regular and fysem courses in each season
def parse_courses(
    season: str,
    aggregate_season_courses: list[dict[str, Any]],
    fysem_courses: set[str],
    data_dir: Path,
    use_cache: bool = True,
) -> list[ParsedCourse]:
    # load from cache if it exists
    if (
        use_cache
        and (
            cache_load := load_cache_json(
                data_dir / "parsed_courses" / f"{season}.json"
            )
        )
        is not None
    ):
        return cache_load

    # parse course JSON in season
    parsed_course_info: list[ParsedCourse] = []
    # not worth parallelizing, already pretty quick
    for x in tqdm(
        aggregate_season_courses, leave=False, desc=f"Parsing season {season}"
    ):
        try:
            parsed_course_info.append(extract_course_info(x, season, fysem_courses))
        except Exception as e:
            print(f"Error parsing course {x['code']} in season {season}: {e}")

    save_cache_json(data_dir / "parsed_courses" / f"{season}.json", parsed_course_info)

    return parsed_course_info

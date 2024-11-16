import re
import traceback
import warnings
from pathlib import Path
from typing import Any, TypedDict, cast

import ujson
from bs4 import BeautifulSoup, MarkupResemblesLocatorWarning, ResultSet, Tag
from tqdm import tqdm
from unidecode import unidecode

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


def normalize_unicode(text: str) -> str:
    unicode_exceptions = {
        r"\u00e2\u20ac\u201c": "–",
        r"\u00c2\u00a0": "\u00a0",
        r"\u00c3\u00a7": "ç",
        r"\u00c3\u00a1": "á",
        r"\u00c3\u00a9": "é",
        r"\u00c3\u00ab": "ë",
        r"\u00c3\u00ae": "î",
        r"\u00c3\u00bc": "ü",
        r"\u00c3\u00b1": "ñ",
        r"\u201c": '"',
        r"\u201d": '"',
    }

    for bad_unicode, replacement in unicode_exceptions.items():
        text = re.sub(bad_unicode, replacement, text)

    # convert utf-8 bytestrings
    # from https://stackoverflow.com/questions/5842115/converting-a-string-which-contains-both-utf-8-encoded-bytestrings-and-codepoints
    text = re.sub(
        r"[\xc2-\xf4][\x80-\xbf]+",
        lambda m: m.group(0).encode("latin1").decode("unicode-escape"),
        text,
    )

    return text


class ParsedProfessors(TypedDict):
    professors: list[str]
    professor_emails: list[str]
    professor_ids: list[str]


def extract_professors(instructordetail_html: str) -> ParsedProfessors:
    soup = BeautifulSoup(normalize_unicode(instructordetail_html), features="lxml")
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
    """
    soup = BeautifulSoup(xlist_html, features="lxml")

    crns = soup.find_all("a", {"data-action": "result-detail"})
    crns = [x["data-key"] for x in crns]
    crns = [x[4:] for x in crns if x[:4] == "crn:"]

    return crns


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


def extract_flags(ci_attrs_html: str) -> list[str]:
    soup = BeautifulSoup(ci_attrs_html, features="lxml")
    flag_texts = [x.get_text() for x in cast(ResultSet[Tag], soup.find_all("a"))]

    return flag_texts


letter_to_day = {
    "Su": 1,
    "M": 2,
    "T(?!h)": 4,  # avoid misidentification as Thursday
    "W": 8,
    "Th": 16,
    "F": 32,
    "Sa": 64,
}


def days_of_week_from_letters(letters: str) -> int:
    if letters == "M-F":
        return 2 + 4 + 8 + 16 + 32

    days: int = 0

    for letter, day in letter_to_day.items():
        if re.search(letter, letters):
            days += day

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


class ParsedMeeting(TypedDict):
    # This is a bitmask, where 1 = Sunday, 2 = Monday, 4 = Tuesday, etc.
    days_of_week: int
    start_time: str
    end_time: str
    location: str
    location_url: str


def extract_meetings(
    meeting_html: str,
    all_in_group: list[dict[str, Any]],
) -> list[ParsedMeeting]:
    if meeting_html == "":
        return extract_meetings_alternate(all_in_group)

    # identify meeting tags and convert to plaintext
    meeting_entries = BeautifulSoup(meeting_html, features="lxml").find_all(
        "div", {"class": "meet"}
    )
    meeting_entries = [x for x in meeting_entries if x.text != "HTBA"]
    if (
        len(meeting_entries) == 0
        or len(meeting_entries) == 1
        and meeting_entries[0].text == "Not Supported"
    ):
        return []

    meetings: list[ParsedMeeting] = []
    for meeting in meeting_entries:
        link = meeting.find("a")
        location_url = link["href"] if link else ""
        days, time, location = split_meeting_text(meeting.text)
        if days == "HTBA":
            continue
        start_time, end_time = time.split("-", 1)
        meetings.append(
            {
                "days_of_week": days_of_week_from_letters(days),
                # Standardize times to 24-hour, full format
                "start_time": format_time(start_time),
                "end_time": format_time(end_time),
                "location": location,
                "location_url": location_url,
            }
        )

    return meetings


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


def extract_meetings_alternate(
    all_in_group: list[dict[str, Any]]
) -> list[ParsedMeeting]:
    """
    Extract course meeting times from the allInGroup key rather than meeting_html. Note that this
    does not return locations because they are not specified.
    """
    if len(all_in_group) == 0:
        return []

    # Use the first listing (for when a course has multiple)
    primary_listing = all_in_group[0]
    if primary_listing["meets"] == "HTBA":
        return []
    meeting_times = ujson.loads(primary_listing["meetingTimes"])
    meetings: dict[tuple[str, str], ParsedMeeting] = {}
    for meeting_time in meeting_times:
        start_time = format_undelimited_time(meeting_time["start_time"])
        end_time = format_undelimited_time(meeting_time["end_time"])
        if (start_time, end_time) in meetings:
            meetings[(start_time, end_time)]["days_of_week"] += 1 << (
                (int(meeting_time["meet_day"]) + 1) % 7
            )
        else:
            meetings[(start_time, end_time)] = {
                "days_of_week": 1 << ((int(meeting_time["meet_day"]) + 1) % 7),
                "start_time": start_time,
                "end_time": end_time,
                "location": "",
                "location_url": "",
            }
    return list(meetings.values())


def extract_resource_link(resources_html: str, title: str) -> str | None:
    matched_link = re.findall(f'href="([^"]*)"[^>]*>{title}</a>', resources_html)

    if len(matched_link) > 0:
        return matched_link[0]
    else:
        return None


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


def extract_skills_areas(yc_attrs_html: str, codes_map: dict[str, str]) -> list[str]:
    codes: list[str] = []

    for search_text, code in codes_map.items():
        if search_text in yc_attrs_html:
            codes.append(code)

    return sorted(codes)


class ParsedDescription(TypedDict):
    requirements: str
    description: str


def extract_prereqs_and_description(description_html: str) -> ParsedDescription:
    raw_description = BeautifulSoup(
        normalize_unicode(description_html), features="lxml"
    )

    # course prerequisites
    prereq_elems = raw_description.findAll("p", {"class": "prerequisites"})
    requirements = "\n".join([x.get_text() for x in prereq_elems]).replace("\r", "")

    # remove prereqs from the description
    for div in prereq_elems:
        div.decompose()

    description_text = raw_description.get_text().rstrip().replace("\r", "")

    return {"requirements": requirements, "description": description_text}


def extract_note(note_html: str) -> str:
    return BeautifulSoup(note_html, features="lxml").get_text().replace("  ", " ")


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
    meetings: list[ParsedMeeting]
    skills: list[str]
    areas: list[str]
    flags: list[str]
    regnotes: str
    rp_attr: str
    classnotes: str
    final_exam: str
    course_home_url: str | None
    syllabus_url: str | None
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
        "title": course_json["title"].replace("\r", ""),
        "school": course_json["col"],
        "credits": extract_credits(
            course_json.get("credit_html", ""), course_json.get("hours", "")
        ),
        "extra_info": STAT_MAP.get(course_json["stat"], "ACTIVE"),
        **extract_professors(course_json["instructordetail_html"]),
        "crn": course_json["crn"],
        "crns": [course_json["crn"], *parse_cross_listings(course_json["xlist"])],
        "course_code": course_json["code"],
        "subject": course_json["code"].split(" ")[0],
        "number": course_json["code"].split(" ")[1],
        "section": course_json["section"].lstrip("0"),
        "meetings": extract_meetings(
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
            traceback.print_exc()

    save_cache_json(data_dir / "parsed_courses" / f"{season}.json", parsed_course_info)

    return parsed_course_info

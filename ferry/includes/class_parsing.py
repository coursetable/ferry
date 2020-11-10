"""
Functions for parsing raw course JSONs used by
/ferry/crawler/parse_classes.py.
"""

import re

import ujson
import unidecode
from bs4 import BeautifulSoup

PROFESSOR_EXCEPTIONS = {
    "Kim Shirkhani": "Kimberly Shirkhani",
    "Derek Green": "John Green",
}


def convert_unicode(text):
    """
    Replace unicode exceptions

    Parameters
    ----------
    text: string

    Returns
    -------
    properly formatted text
    """

    # handle incorrectly coded em dash

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
    }

    for bad_unicode, replacement in unicode_exceptions.items():
        text = re.sub(bad_unicode, replacement, text)

    # convert utf-8 bytestrings
    # pylint: disable=line-too-long
    # from https://stackoverflow.com/questions/5842115/converting-a-string-which-contains-both-utf-8-encoded-bytestrings-and-codepoints
    text = re.sub(
        r"[\xc2-\xf4][\x80-\xbf]+",
        lambda m: m.group(0).encode("latin1").decode("unicode-escape"),
        text,
    )

    return text


def professors_from_html(html):
    """
    Parse course instructors from provided HTML field

    Parameters
    ----------
    html: string
        HTML containing instructors

    Returns
    -------
    names: list
        course instructors
    """

    soup = BeautifulSoup(html, features="lxml")
    instructor_divs = soup.findAll("div", {"class": "instructor-detail"})

    names = []
    emails = []
    ids = []  # Yale course search's internal professor ID

    for div in instructor_divs:

        instructor_name = div.find("div", {"class": "instructor-name"})
        instructor_email = div.find("div", {"class": "instructor-email"})
        instructor_id = ""  # default

        if instructor_name:

            # check if the professor has an associated ID
            instructor_search = instructor_name.find("a", {"data-action": "search"})

            if instructor_search:
                instructor_id = instructor_search["data-id"]

            # extract the name in plaintext
            instructor_name = instructor_name.get_text()
        else:
            instructor_name = ""

        if instructor_email:

            # extract the email in plaintext
            instructor_email = instructor_email.get_text()
        else:
            instructor_email = ""

        # remove accents from professor names
        instructor_name = unidecode.unidecode(instructor_name)

        # patch certain professor names manually
        if instructor_name in PROFESSOR_EXCEPTIONS.keys():
            instructor_name = PROFESSOR_EXCEPTIONS[instructor_name]

        # if the professor has a name and is not listed as staff, add it
        if len(instructor_name) > 0 and instructor_name != "Staff":
            names.append(instructor_name)
            emails.append(instructor_email)
            ids.append(instructor_id)

    # skip sorting and return empty
    if len(names) == 0:
        return [], [], []

    # parallel sort by instructor name
    names, emails, ids = zip(*sorted(zip(names, emails, ids)))

    return names, emails, ids


def parse_cross_listings(xlist_html):
    """
    Retrieve cross-listings (CRN codes) from the HTML in
    the 'xlist' field from the Yale API

    Note that the cross-listings do not include the course
    itself

    Parameters
    ----------
    xlist_html: string
        'xlist' field from the Yale API response

    Returns
    -------
    xlist_crns: CRN codes of course cross-listings
    """

    xlist_soup = BeautifulSoup(xlist_html, features="lxml")

    xlist_crns = xlist_soup.find_all("a", {"data-action": "result-detail"})
    xlist_crns = [x["data-key"] for x in xlist_crns]
    xlist_crns = [x[4:] for x in xlist_crns if x[:4] == "crn:"]

    return xlist_crns


def extract_flags(ci_attrs):
    """
    Get the course flags from the ci_attrs field

    Parameters
    ----------
    ci_attrs: string
        the field from the Yale API response

    Returns
    -------
    flag_texts: list of strings
        flags (keywords) for the course
    """

    soup = BeautifulSoup(ci_attrs, features="lxml")
    flag_texts = [x.get_text() for x in soup.find_all("a")]

    return flag_texts


def days_of_week_from_letters(letters):
    """
    Parse course days from letterings

    Parameters
    ----------
    letters: string
        Course meeting days, abbreviated

    Returns
    -------
    days: list of strings
        Days on which course meets
    """

    if letters == "M-F":
        return ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday"]

    days = []

    letter_to_day = {
        "M": "Monday",
        "(T[^h]|T$)": "Tuesday",  # avoid misidentification as Thursday
        "W": "Wednesday",
        "Th": "Thursday",
        "F": "Friday",
        "Sa": "Saturday",
        "Su": "Saturday",
    }

    letters = letters + " "

    for letter, day in letter_to_day.items():
        if re.search(letter, letters):
            days.append(day)

    return days


def format_time(time):
    """
    Convert Yale API times to 24-hour, full format

    Parameters
    ----------
    time: string
        a time from the Yale API 'meeting_html' field

    Returns
    -------
    time: string
        formatted time

    """

    time_stripped = time[:-2]

    if ":" in time_stripped:

        hour = time_stripped.split(":")[0]
        minute = time_stripped.split(":")[1]

    else:

        hour = time_stripped
        minute = "00"

    hour = int(hour)

    if time[-2:] == "am" and hour == 12:
        hour = 0

    elif time[-2:] == "pm" and 1 <= hour <= 11:
        hour = hour + 12

    hour = str(hour)

    return hour + ":" + minute


def extract_split_meetings(meetings):
    """
    Split meeting strings into [days, time, location] tuples.

    Used in extract_meetings()

    Parameters
    ----------
    meetings: list of strings
        meeting strings from extract_meetings

    Returns
    -------
    split meeting strings into days, time, location

    """

    split_meetings = []

    # split meetings by time
    for meeting in meetings:

        split_meeting = [meeting, "", ""]

        if " in " in meeting:

            sessions, location = meeting.split(" in ")[:2]

            if " " in sessions:

                days, time = sessions.split(" ")[:2]

                split_meeting = [days, time, location]

            else:

                split_meeting = ["HTBA", "", location]

        elif " " in meeting:

            days, time = meeting.split(" ")[:2]

            split_meeting = [days, time, ""]

        else:

            split_meeting = [meeting, "", ""]

        split_meetings.append(split_meeting)

    return split_meetings


def extract_meeting_summaries(meetings, split_meetings):

    """
    Get meeting time and location summary strings.

    Used in extract_meetings()

    Parameters
    ----------
    meetings: list of strings
        meeting strings from extract_meetings

    split_meetings: list of meeting [days, time, location]
        split meetings from extract_split_meetings()

    Returns
    -------
    times_summary, locations_summary
    """

    # produce long_summary
    times_long_summary = "\n".join(meetings)

    # make times summary as first listed
    times_summary = split_meetings[0][0] + " " + split_meetings[0][1]

    # collapse additional times
    if len(split_meetings) > 1:
        times_summary = times_summary + f" + {len(split_meetings)-1}"

    # make locations summary as first listed
    locations_summary = split_meetings[0][2]
    # locations_summary = location_urls[0] if len(location_urls) else ""

    # collapse additional locations
    if len(split_meetings) > 1:
        locations_summary = locations_summary + f" + {len(split_meetings)-1}"

    # some final touches
    times_summary = times_summary.replace("MTWThF", "M-F")
    locations_summary = locations_summary.replace("MTWThF", "M-F")
    times_long_summary = times_long_summary.replace("MTWThF", "M-F")

    if locations_summary == "" or locations_summary[:3] == " + ":
        locations_summary = "TBA"

    # handle redundant dash-delimited format (introduced in fall 2020)
    if locations_summary.count(" - ") == 1:

        locations_1, locations_2 = locations_summary.split(" - ")

        # if location is redundant
        if locations_2.startswith(locations_1):

            locations_summary = locations_2

    return times_summary, locations_summary, times_long_summary


def extract_formatted_meetings(split_meetings, location_urls):

    """
    Extract formatted meetings in
        {
            days: [days]
            start_time:
            end_time:
            location:
        }
    format.

    Used in extract_meetings()

    Parameters
    ----------
    split_meetings: list of meeting [days, time, location]
        split meetings from extract_split_meetings()
    location_urls: list of strings
        links to Yale map of meeting buildings

    Returns
    -------
    formatted_meetings
    """

    formatted_meetings = []

    for meeting in split_meetings:
        location_indx = 0  # variable to loop through location_urls list
        if meeting[0] == "HTBA":

            formatted_meetings.append(
                {"days": [], "start_time": "", "end_time": "", "location": ""}
            )

        else:

            days = meeting[0]
            times = meeting[1]
            location = meeting[2]
            location_url = (
                location_urls[location_indx]
                if location_indx < len(location_urls)
                else ""
            )
            location_indx += 1

            days = days_of_week_from_letters(days)

            times = times.split("-")
            start_time = times[0]
            end_time = times[1]

            # standardize times to 24-hour, full format
            start_time = format_time(start_time)
            end_time = format_time(end_time)

            formatted_meetings.append(
                {
                    "days": days,
                    "start_time": start_time,
                    "end_time": end_time,
                    "location": location,
                    "location_url": location_url,
                }
            )

    return formatted_meetings


def extract_meetings_by_day(formatted_meetings):

    """
    Transform formatted meetings in
        {
            days: [days]
            start_time:
            end_time:
            location:
        }
    format to
        {
            day: [
                [start_time, end_time, location, location_url]
                ...
            ]
        }
    format.

    Used in extract_meetings()

    Parameters
    ----------
    formatted_meetings: list of dictionaries
        formatted meetings from extract_formatted_meetings()

    Returns
    -------
    meetings_by_day
    """

    meetings_by_day = dict()

    for meeting in formatted_meetings:

        for day in meeting["days"]:

            session = [
                meeting["start_time"],
                meeting["end_time"],
                meeting["location"],
                meeting.get("location_url", ""),
            ]

            # if day key already present, append
            if day in meetings_by_day.keys():
                meetings_by_day[day].append(session)
            # otherwise, initialize list
            else:
                meetings_by_day[day] = [session]

    return meetings_by_day


def extract_meetings(meeting_html):
    """
    Extract course meeting times and locations from the
    provided HTML

    Parameters
    ----------
    meeting_html: string
        HTML of course meeting times, specified by
        the 'meeting_html' key in the Yale API response

    Returns
    -------
    formatted_meetings: list of dictionaries
        list of course meeting dates/times, each with keys
        'days', 'start_time', 'end_time', 'location'
    times_summary: string
        summarization of meeting times; the first listed
        times are shown while additional ones are collapsed
        to " + (n-1)"
    locations_summary: string
        summarization of meeting locations; the first listed
        locations are shown while additional ones are collapsed
        to " + (n-1)"
    times_long_summary: string
        summary of meeting times and locations; computed as
        comma-joined texts from the meeting_html items
    meetings_by_day: dictionary
        dictionary with keys as days and values consisting of
        lists of [start_time, end_time, location]

    """

    # identify meeting tags and convert to plaintext
    meetings = BeautifulSoup(meeting_html, features="lxml")

    # list that holds the urls of each location
    location_urls = []
    for meeting_link in meetings.find_all("a", href=True):
        location_urls.append(meeting_link["href"])

    meetings = meetings.find_all("div", {"class": "meet"})
    meetings = [x.text for x in meetings]

    if len(meetings) == 0 or meetings[0] == "HTBA":

        formatted_meetings = [
            {"days": [], "start_time": "", "end_time": "", "location": ""}
        ]

        return formatted_meetings, "TBA", "TBA", "TBA", {}

    split_meetings = extract_split_meetings(meetings)
    times_summary, locations_summary, times_long_summary = extract_meeting_summaries(
        meetings, split_meetings
    )

    formatted_meetings = extract_formatted_meetings(split_meetings, location_urls)

    meetings_by_day = extract_meetings_by_day(formatted_meetings)

    return (
        formatted_meetings,
        times_summary,
        locations_summary,
        times_long_summary,
        meetings_by_day,
    )


def format_undelimited_time(time):
    """
    Convert an undelimited time string (e.g. "1430") to a
    delimited one (e.g. "14:30")
    """

    hours, minutes = time[:-2], time[-2:]

    return f"{hours}:{minutes}"


days_map = {
    "0": "Monday",
    "1": "Tuesday",
    "2": "Wednesday",
    "3": "Thursday",
    "4": "Friday",
    "5": "Saturday",
    "6": "Sunday",
}


def extract_meetings_alternate(course_json):

    """
    Extract course meeting times from the allInGroup key rather than
    meeting_html. Note that this does not return locations because they are
    not specified.

    Parameters
    ----------
    course_json: dict
        course_json object read from course_json_cache

    Returns
    -------
    times_summary: string
        summarization of meeting times; the first listed
        times are shown while additional ones are collapsed
        to " + (n-1)"
    locations_summary: string
        summarization of meeting locations; the first listed
        locations are shown while additional ones are collapsed
        to " + (n-1)"
    times_long_summary: string
        summary of meeting times and locations; computed as
        comma-joined texts from the meeting_html items
    times_by_day: dictionary
        dictionary with keys as days and values consisting of
        lists of [start_time, end_time, location]

    """

    locations_summary = "TBA"
    times_by_day = dict()

    # check if there is a valid listing

    listings = course_json.get("allInGroup", "[]")

    if len(listings) >= 0:

        # use the first listing (for when a course has multiple)

        primary_listing = listings[0]

        times_summary = primary_listing["meets"]

        if times_summary == "HTBA":
            times_summary = "TBA"

        meeting_times = ujson.loads(primary_listing["meetingTimes"])

        for meeting_time in meeting_times:

            meeting_day = days_map[meeting_time["meet_day"]]

            session = [
                format_undelimited_time(meeting_time["start_time"]),
                format_undelimited_time(meeting_time["end_time"]),
                "",
                "",
            ]

            if meeting_day in times_by_day.keys():
                times_by_day[meeting_day].append(session)

            else:
                times_by_day[meeting_day] = [session]

    # if no valid listing, then return the default missing values
    else:

        times_summary = "TBA"

    # since there are no locations, just set this to times_summary
    times_long_summary = times_summary

    return (
        times_summary,
        locations_summary,
        times_long_summary,
        times_by_day,
    )


# abbreviations for skills
skills_map = {
    "Writing": "WR",
    "Quantitative Reasoning": "QR",
    "Language (1)": "L1",
    "Language (2)": "L2",
    "Language (3)": "L3",
    "Language (4)": "L4",
    "Language (5)": "L5",
}

# abbreviations for areas
areas_map = {
    "Humanities": "Hu",
    "Social Sciences": "So",
    ">Sciences": "Sc",
}

# abbreviations for course statuses
stat_map = {
    "A": "ACTIVE",
    "B": "MOVED_TO_SPRING_TERM",
    "C": "CANCELLED",
    "D": "MOVED_TO_FALL_TERM",
    "E": "CLOSED",
    "N": "NUMBER_CHANGED",
}


def found_items(text, mapping):
    """
    Given an input string, see if any of the
    keys in the provided mapping are present. If so,
    for each key return the matched value. (useful for
    fetching skills and areas from codes)

    Parameters
    ----------
    text: string
        Text to search over

    mapping: dict
        Keys+values to pull from text

    Returns
    -------
    items: list
        Encoded values found in the text
    """

    items = []

    for search_text, code in mapping.items():
        if search_text in text:
            items.append(code)

    return sorted(items)


def extract_prereqs(raw_description):
    """
    Parse prerequisites from description beautifulsoup.

    Parameters
    ----------
    raw_description: BeautifulSoup object
        BeautifulSoup of course description HTML

    Returns
    -------
    prerequisites text
    """

    # course prerequisites
    prereqs = raw_description.findAll("p", {"class": "prerequisites"})
    if len(prereqs) >= 1:
        prereqs = "\n".join([x.get_text() for x in prereqs])

        return prereqs

    return ""


def extract_course_info(course_json, season, fysem):
    """
    Parse the JSON response from the Yale courses API
    into a more useful format

    Parameters
    ----------
    course_json: dict
        JSON response from courses API
    season: string
        The season that the courses belong to
        (required because not returned by the
        Yale API)

    Returns
    -------
    course_info: dict
        Processed course information
    """

    course_info = {}

    course_info["season_code"] = season

    description_html = course_json["description"]

    raw_description = BeautifulSoup(convert_unicode(description_html), features="lxml")

    course_info["requirements"] = extract_prereqs(raw_description)

    # remove prereqs from the description
    for div in raw_description.find_all("p", {"class": "prerequisites"}):
        div.decompose()

    description_text = raw_description.get_text().rstrip()

    # Course description
    course_info["description"] = description_text

    # Course title
    truncate_title = lambda x: f"{x[:29]}..." if len(x) > 32 else x
    course_info["short_title"] = truncate_title(course_json["title"])

    course_info["title"] = course_json["title"]

    # Course school (Yale College, SOM, graduate, etc.)
    course_info["school"] = course_json["col"]

    # Number of credits

    # non-Yale College courses don't have listed credits, so assume they are 1
    if course_json["hours"] == "":
        course_info["credits"] = 1
    else:
        course_info["credits"] = float(course_json["hours"])

    # Course status
    course_info["extra_info"] = stat_map.get(course_json["stat"], "ACTIVE")

    # Instructors
    (
        course_info["professors"],
        course_info["professor_emails"],
        course_info["professor_ids"],
    ) = professors_from_html(convert_unicode(course_json["instructordetail_html"]))

    # CRN
    course_info["crn"] = course_json["crn"]

    # Cross-listings
    course_info["crns"] = [
        course_info["crn"],
        *parse_cross_listings(course_json["xlist"]),
    ]

    # Subject, numbering, and section
    course_info["course_code"] = course_json["code"]
    course_info["subject"] = course_json["code"].split(" ")[0]
    course_info["number"] = course_json["code"].split(" ")[1]
    course_info["section"] = course_json["section"].lstrip("0")

    # Meeting times
    if course_json.get("meeting_html", "") != "":

        (
            # course_info["extracted_meetings"],
            _,
            course_info["times_summary"],
            course_info["locations_summary"],
            course_info["times_long_summary"],
            course_info["times_by_day"],
        ) = extract_meetings(course_json["meeting_html"])

    # Fall 2020 courses do not have meeting_htmls because most are online
    else:

        (
            course_info["times_summary"],
            course_info["locations_summary"],
            course_info["times_long_summary"],
            course_info["times_by_day"],
        ) = extract_meetings_alternate(course_json)

    # Skills and areas
    course_info["skills"] = found_items(course_json["yc_attrs"], skills_map)
    course_info["areas"] = found_items(course_json["yc_attrs"], areas_map)

    # additional info fields
    course_info["flags"] = extract_flags(course_json["ci_attrs"])
    course_info["regnotes"] = (
        BeautifulSoup(course_json["regnotes"], features="lxml")
        .get_text()
        .replace("  ", " ")
    )
    course_info["rp_attr"] = (
        BeautifulSoup(course_json["rp_attr"], features="lxml")
        .get_text()
        .replace("  ", " ")
    )
    course_info["classnotes"] = (
        BeautifulSoup(course_json.get("clssnotes", ""), features="lxml")
        .get_text()
        .replace("  ", " ")
    )
    course_info["final_exam"] = course_json["final_exam"]

    # Course homepage
    matched_homepage = re.findall(
        'href="([^"]*)"[^>]*>HOMEPAGE</a>', course_json["resources"]
    )

    if len(matched_homepage) > 0:
        course_info["course_home_url"] = matched_homepage[0]
    else:
        course_info["course_home_url"] = ""

    # Link to syllabus
    matched_syllabus = re.findall(
        'href="([^"]*)"[^>]*>SYLLABUS</a>', course_json["resources"]
    )

    if len(matched_syllabus) > 0:
        course_info["syllabus_url"] = matched_syllabus[0]
    else:
        course_info["syllabus_url"] = ""

    # if first-year seminar
    course_info["fysem"] = course_info["crn"] in fysem

    return course_info
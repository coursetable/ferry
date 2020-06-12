import requests
import ujson

from bs4 import BeautifulSoup

import unidecode
import re

import dateutil.parser
import time
from datetime import datetime
import calendar

class FetchClassesError(Exception):
    pass

def fetch_seasons():
    """
    Get list of seasons

    Returns
    -------
    seasons: list of seasons
    """

    r = requests.post("https://courses.yale.edu/")

    # Successful response
    if r.status_code == 200:

        seasons = re.findall('option value="(\d{6})"', r.text)

        # exclude '999999' catch-all 'Past seasons' season option

        seasons = sorted([x for x in seasons if x != '999999'])

        return seasons

    # Unsuccessful
    else:
        raise FetchClassesError(f'Unsuccessful response: code {r.status_code}')


def fetch_all_api_seasons():
    """
    Get full list of seasons for the 
    Yale official course API

    Returns
    -------
    seasons: list of seasons
    """

    recent_seasons = fetch_seasons()

    oldest_season = int(recent_seasons[0][:4])

    # oldest season is 201903
    previous_seasons = [str(x) for x in range(2010, oldest_season+1)]
    previous_seasons = [[x+"01", x+"02", x+"03"] for x in previous_seasons]

    # flatten
    previous_seasons = [x for y in previous_seasons for x in y]
    previous_seasons = ["200903"] + previous_seasons
    seasons = sorted(list(set(recent_seasons) | set(previous_seasons)))

    return seasons


def fetch_season_subjects(season, api_key):
    """
    Get list of course subjects in a season,
    needed for querying the courses later

    Parameters
    ----------
    season: string
        The season to to get courses for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)
    api_key: string
        API key with access to the Yale CourseSubjects API
        (see https://developers.yale.edu/coursesubjects)

    Returns
    -------
    subjects: JSON of season subjects
    """

    url_template = "https://gw.its.yale.edu/soa-gateway/course/webservice/subjects?termCode={}&mode=json&apiKey={}"
    url = url_template.format(season, api_key)

    r = requests.get(url)

    # Successful response
    if r.status_code == 200:

        subjects = ujson.loads(r.text)

        return subjects

    # Unsuccessful
    else:
        raise FetchClassesError(f'Unsuccessful response: code {r.status_code}')


def fetch_season_subject_courses(season, subject, api_key):
    """
    Get courses in a season, for a given subject

    Parameters
    ----------
    season: string
        The season to to get courses for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)
    subject: string
        Subject to get courses for. For instance, "CPSC"
    api_key: string
        API key with access to the Yale Courses API
        (see https://developers.yale.edu/courses)

    Returns
    -------
    subject_courses: JSON of course subjects
    """

    url_template = "https://gw.its.yale.edu/soa-gateway/course/webservice/index?termCode={}&subjectCode={}&mode=json&apiKey={}"
    url = url_template.format(season, subject, api_key)

    r = requests.get(url)

    # Successful response
    if r.status_code == 200:

        courses = ujson.loads(r.text)

        return courses

    # Unsuccessful
    else:
        raise FetchClassesError(f'Unsuccessful response: code {r.status_code}')


def fetch_season_courses(season):
    """
    Get preliminary course info for a given season

    Parameters
    ----------
    season: string
        The season to to get courses for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)

    Returns
    -------
    r: JSON-formatted course information
    """

    url = "https://courses.yale.edu/api/?page=fose&route=search"

    payload = {'other': {'srcdb': season}, 'criteria': []}

    r = requests.post(url, data=ujson.dumps(payload))

    # Successful response
    if r.status_code == 200:

        r_json = ujson.loads(r.text)

        if "fatal" in r_json.keys():
            raise FetchClassesError(f'Unsuccessful response: {r_json["fatal"]}')

        if "results" not in r_json.keys():
            raise FetchClassesError('Unsuccessful response: no results')

        return r_json["results"]

    # Unsuccessful
    else:
        raise FetchClassesError(f'Unsuccessful response: code {r.status_code}')


def fetch_previous_json(season, evals=False):
    """
    Get existing JSON files for a season from the CourseTable website
    (at https://coursetable.com/gen/json/data_with_evals_<season_CODE>.json)

    Parameters
    ----------
    season: string
        The season to to get courses for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)

    Returns
    -------
    r: JSON-formatted course information
    """

    if evals:
        url = f"https://coursetable.com/gen/json/data_with_evals_{season}.json"
    elif not evals:
        url = f"https://coursetable.com/gen/json/data_{season}.json"

    r = requests.get(url)

    # Successful response
    if r.status_code == 200:

        r_json = ujson.loads(r.text)

        return r_json

    # Unsuccessful
    else:
        raise FetchClassesError('Unsuccessful response: code {}'.format(r.status_code))


def fetch_course_json(code, crn, srcdb):
    """
    Fetch information for a course from the API

    Parameters
    ----------
    code: string
        the course code
    crn: string
        the course registration number
    srcdb: string
        season the course is in

    Returns
    -------
    course_json: dict
        JSON-formatted course information
    """

    url = "https://courses.yale.edu/api/?page=fose&route=details"

    payload = {
        "group": "code:" + code + "",
        "key": "crn:" + crn + "",
        "srcdb": "" + srcdb + "",
        "matched": "crn:" + crn + ""
    }

    r = requests.post(url, data=ujson.dumps(payload))

    # Successful response
    if r.status_code == 200:

        course_json = ujson.loads(r.text)

        if "fatal" in course_json.keys():
            raise FetchClassesError(
                'Unsuccessful response: {}'.format(course_json["fatal"]))

        return course_json

    # Unsuccessful
    else:
        raise FetchClassesError('Unsuccessful response: code {}'.format(r.status_code))


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
    matched_divs = soup.findAll("div", {"class": "instructor-name"})

    names = []

    for div in matched_divs:

        text = div.get_text()

        # disregard email fields
        if "mailto:" not in text:

            # remove accents from professor names
            name = unidecode.unidecode(text)

            if len(name) > 0 and name != "Staff":
                names.append(name)

    return names


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
        return ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']

    days = []

    letter_to_day = {
        "M": "Monday",
        "(T[^h]|T$)": "Tuesday",  # avoid misidentification as Thursday
        "W": "Wednesday",
        "Th": "Thursday",
        "F": "Friday"
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

    if time[-2:] == "pm":
        hour = str((int(hour) + 12) % 24)

    return hour + ":" + minute


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
    extracted_times: list of dictionaries
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
    times_by_day: dictionary
        dictionary with keys as days and values consisting of
        lists of [start_time, end_time, location]

    """

    # identify meeting tags and convert to plaintext
    meetings = BeautifulSoup(meeting_html, features="lxml")
    meetings = meetings.find_all("div", {"class": "meet"})
    meetings = [x.text for x in meetings]

    only_htba = False

    if len(meetings) == 0:
        only_htba = True
    else:
        only_htba = meetings[0] == "HTBA"

    # if no meetings found
    if only_htba:

        extracted_meetings = [{
            "days": [],
            "start_time":"",
            "end_time":"",
            "location":""
        }]

        return extracted_meetings, "TBA", "TBA", "TBA", {}

    # produce long_summary
    times_long_summary = "\n".join(meetings)

    # split meetings by time
    for idx, meeting in enumerate(meetings):

        if " in " in meeting:

            sessions, location = meeting.split(" in ")[:2]

            if " " in sessions:

                days, time = sessions.split(" ")[:2]

                meetings[idx] = [days, time, location]

            else:

                meetings[idx] = ["HTBA", "", location]

        elif " " in meeting:

            days, time = meeting.split(" ")[:2]

            meetings[idx] = [days, time, ""]

        else:

            meetings[idx] = [meeting, "", ""]

    # make times summary as first listed
    times_summary = meetings[0][0]+" "+meetings[0][1]

    # collapse additional times
    if len(meetings) > 1:
        times_summary = times_summary + f" + {len(meetings)-1}"

    # make locations summary as first listed
    locations_summary = meetings[0][2]

    # collapse additional locations
    if len(meetings) > 1:
        locations_summary = locations_summary + f" + {len(meetings)-1}"

    extracted_meetings = []

    for meeting in meetings:

        if meeting[0] == "HTBA":

            extracted_meetings.append({
                "days": [],
                "start_time": "",
                "end_time": "",
                "location": ""
            })

        else:

            days = meeting[0]
            times = meeting[1]
            location = meeting[2]

            days = days_of_week_from_letters(days)

            times = times.split("-")
            start_time = times[0]
            end_time = times[1]

            # standardize times to 24-hour, full format
            start_time = format_time(start_time)
            end_time = format_time(end_time)

            extracted_meetings.append({
                "days": days,
                "start_time": start_time,
                "end_time": end_time,
                "location": location
            })

    times_by_day = dict()

    for meeting in extracted_meetings:

        for day in meeting["days"]:

            session = [
                meeting["start_time"],
                meeting["end_time"],
                meeting["location"]
            ]

            # if day key already present, append
            if day in times_by_day.keys():
                times_by_day[day].append(session)
            # otherwise, initialize list
            else:
                times_by_day[day] = [session]

    return extracted_meetings, times_summary, locations_summary, times_long_summary, times_by_day


# abbreviations for skills
skills_map = {
    'Writing': 'WR',
    'Quantitative Reasoning': 'QR',
    'Language (1)': 'L1',
    'Language (2)': 'L2',
    'Language (3)': 'L3',
    'Language (4)': 'L4',
    'Language (5)': 'L5'
}

# abbreviations for areas
areas_map = {
    'Humanities': 'Hu',
    'Social Sciences': 'So',
    '>Sciences': 'Sc',
}

# abbreviations for course statuses
stat_map = {
    "A": "ACTIVE",
    "B": "MOVED_TO_SPRING_TERM",
    "C": "CANCELLED",
    "D": "MOVED_TO_FALL_TERM",
    "E": "CLOSED",
    "N": "NUMBER_CHANGED"
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

    return items


def extract_course_info(course_json, season):
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

    raw_description = BeautifulSoup(
        course_json["description"], features="lxml")

    # Course description
    course_info["description"] = raw_description.get_text()

    # Course prerequisites
    prereqs = raw_description.findAll("p", {"class": "prerequisites"})
    if len(prereqs) == 1:
        course_info["requirements"] = prereqs[-1].get_text()
    else:
        course_info["requirements"] = ""

    # Add course credits to description field
    if course_json["hours"] != "1" and course_json["hours"] != "":
        course_info["description"] += f"\n\n{course_json['hours']} Yale College course credits"

    # Course title
    if len(course_json["title"]) > 32:
        course_info["short_title"] = course_json["title"][:29] + "..."
    else:
        course_info["short_title"] = course_json["title"]

    course_info["title"] = course_json["title"]

    # Course status
    course_info["extra_info"] = stat_map.get(course_json["stat"], "")

    # Instructors
    course_info["professors"] = professors_from_html(
        course_json["instructordetail_html"])

    # CRN
    course_info["crn"] = course_json["crn"]

    # Cross-listings
    course_info["crns"] = [course_info["crn"], *parse_cross_listings(course_json["xlist"])]

    # Subject, numbering, and section
    course_info["course_code"] = course_json["code"]
    course_info["subject"] = course_json["code"].split(" ")[0]
    course_info["number"] = course_json["code"].split(" ")[1]
    course_info["section"] = course_json['section'].lstrip("0")

    # Meeting times
    (
        # course_info["extracted_meetings"],
        _,
        course_info["times_summary"],
        course_info["locations_summary"],
        course_info["times_long_summary"],
        course_info["times_by_day"],
    ) = extract_meetings(course_json["meeting_html"])

    # Skills and areas
    course_info["skills"] = found_items(course_json["yc_attrs"],
                                        skills_map)
    course_info["areas"] = found_items(course_json["yc_attrs"],
                                       areas_map)

    course_info["flags"] = extract_flags(course_json["ci_attrs"])

    # Course homepage
    matched_homepage = re.findall(
        'href="([^"]*)"[^>]*>HOMEPAGE</a>', course_json["resources"])

    if len(matched_homepage) > 0:
        course_info["course_home_url"] = matched_homepage[0]
    else:
        course_info["course_home_url"] = ''

    # Link to syllabus
    matched_syllabus = re.findall(
        'href="([^"]*)"[^>]*>SYLLABUS</a>', course_json["resources"])

    if len(matched_syllabus) > 0:
        course_info["syllabus_url"] = matched_syllabus[0]
    else:
        course_info["syllabus_url"] = ''

    return course_info

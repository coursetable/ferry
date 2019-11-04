import requests
import json

from bs4 import BeautifulSoup

import unidecode
import re

import dateutil.parser
import time
from datetime import datetime
import calendar


def fetch_terms():
    """
    Get list of seasons

    Returns
    -------
    seasons: list of seasons
    """

    r = requests.post("https://courses.yale.edu/")

    # Successful response
    if r.status_code == 200:

        terms = re.findall('option value="(\d{6})"', r.text)

        # exclude '999999' catch-all 'Past Terms' term option

        terms = [x for x in terms if x != '999999']

        return terms

    # Unsuccessful
    else:
        raise Exception('Unsuccessful response: code {}'.format(r.status))


def fetch_term_courses(term):
    """
    Get preliminary course info for a given term

    Parameters
    ----------
    term: string
        The term to to get courses for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)

    Returns
    -------
    r: JSON-formatted course information
    """

    url = "https://courses.yale.edu/api/?page=fose&route=search"

    payload = {'other': {'srcdb': term}, 'criteria': []}

    r = requests.post(url, data=json.dumps(payload))

    # Successful response
    if r.status_code == 200:

        r_json = json.loads(r.text)

        if "fatal" in r_json.keys():
            raise Exception(
                'Unsuccessful response: {}'.format(r_json["fatal"]))

        if "results" not in r_json.keys():
            raise Exception('Unsuccessful response: no results')

        return r_json["results"]

    # Unsuccessful
    else:
        raise Exception('Unsuccessful response: code {}'.format(r.status))


def fetch_previous_json(term, evals=False):
    """
    Get existing JSON files for a term from the CourseTable website
    (at https://coursetable.com/gen/json/data_with_evals_<TERM_CODE>.json)

    Parameters
    ----------
    term: string
        The term to to get courses for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)

    Returns
    -------
    r: JSON-formatted course information
    """

    if evals:
        url = "https://coursetable.com/gen/json/data_with_evals_{}.json".format(
            term)
    elif not evals:
        url = "https://coursetable.com/gen/json/data_{}.json".format(
            term)

    r = requests.get(url)

    # Successful response
    if r.status_code == 200:

        r_json = json.loads(r.text)

        return r_json

    # Unsuccessful
    else:
        raise Exception('Unsuccessful response: code {}'.format(r.status))


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
        term the course is in

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

    r = requests.post(url, data=json.dumps(payload))

    # Successful response
    if r.status_code == 200:

        course_json = json.loads(r.text)

        if "fatal" in course_json.keys():
            raise Exception(
                'Unsuccessful response: {}'.format(course_json["fatal"]))

        return course_json

    # Unsuccessful
    else:
        raise Exception('Unsuccessful response: code {}'.format(r.status))


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


def course_codes_from_fields(code, xlist, section, crn):
    """
    Parse course code from fields

    Parameters
    ----------
    code: string
        The primary course code
    xlist: string
        The cross-listed course codes
    section: string
        The course section
    crn: string
        The course registration number identifier

    Returns
    -------
    names: dict
        Course codes information
    """

    primary_course_code = code.split(" ")

    course_codes = [
        {"subject": primary_course_code[0],
         "number": primary_course_code[1]
         }
    ]

    return {
        "oci_id": crn,
        "section": section,
        "listings": course_codes
    }


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


def time_of_day_float_from_string(time_string):
    """
    Convert a string formatted time to a float format,
    with hours to the left of the decimal point and minutes
    to the right

    Parameters
    ----------
    time_string: string
        Course letterings

    Returns
    -------
    time: float
        time of day, float-formatted
    """

    # split time string into hour/minute/AM-PM sections
    matches = list(re.findall('([0-9]*):?([0-9]*)(am|pm)', time_string)[0])

    hours = int(matches[0])

    # convert from AM/PM to 24-hour format
    if matches[2] == "pm" and hours != 12:
        hours += 12

    # no minutes stated equivalent to zero
    if len(matches[1]) == 0:
        matches[1] = "0"

    time = hours + int(matches[1])/100

    return time


def course_times_from_fields(meeting_html, all_sections_remove_children):
    """
    Get the course meeting times from provided HTML

    Parameters
    ----------
    meeting_html: string
        HTML of course meeting times
    all_sections_remove_children
        Same parameter as provided in JSON

    Returns
    -------
    meeting_times: list of dictionaries
        course meeting dates/times
    """

    soup = BeautifulSoup(meeting_html, features="lxml")
    meetings = soup.find_all("div")
    found_htba = False
    htba_course_time = {
        "days": ["HTBA"],
        "start_time": "1",
        "end_time": "1",
        "location": ""
    }

    matched_meetings = []

    for meeting in meetings:
        meeting_text = "".join(meeting.find_all(text=True))

        if len(meeting_text) == 0:
            pass

        if "HTBA" in meeting_text:

            matched_meetings.append(htba_course_time)

            found_htba = True

        else:
            meeting_parts = meeting_text.split(" ")

            days = days_of_week_from_letters(meeting_parts[0])

            times = meeting_parts[1].split("-")
            start = time_of_day_float_from_string(times[0])
            end = time_of_day_float_from_string(times[1])

            location_matches = re.findall(' in ([^<]*)', meeting_text)

            if len(location_matches) > 0:
                location = location_matches[0]
            else:
                location = ''

            matched_meetings.append({
                "days": days,
                "start_time": start,
                "end_time": end,
                "location": location
            })

    if not found_htba and len(matched_meetings) == 0 and "HTBA" in all_sections_remove_children:
        matched_meetings.append(htba_course_time)

    return matched_meetings


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


def exam_from_field(final_exam):
    """
    Pull final exam date/time information from
    provided string

    Parameters
    ----------
    final_exam: string
        Final exam information 

    Returns
    -------
    exam_info: dict
        Date, day of week, and time of final exam
    """

    no_final = "No regular final examination" in final_exam

    if len(final_exam) == 0 or no_final:
        return {
            'group': 0,
            'date': '1000-01-01',
            'day_of_week': '',
            'time': 0.0
        }

    if "HTBA" in final_exam:
        return {
            'group': 1,
            'date': '1000-01-01',
            'day_of_week': '',
            'time': 0.0
        }

    split_date_time = final_exam.split(" at ")

    date = dateutil.parser.parse(split_date_time[0])

    # encode date as UNIX time
    date_unix = int(time.mktime(date.timetuple()))
    date_unix = datetime.fromtimestamp(date_unix)

    # encode exam time as <hours>.<minutes> format
    time_float = time_of_day_float_from_string(split_date_time[1])

    return {
        'group': 2,
        'date': date_unix.strftime('%Y-%m-%d'),
        'day_of_week': calendar.day_name[date_unix.weekday()],
        'time': time_float
    }


def extract_course_info(course_json):
    """
    Parse the JSON response from the Yale courses API
    into a more useful format

    Parameters
    ----------
    course_json: dict
        JSON response from courses API

    Returns
    -------
    course_info: dict
        Processed course information
    """

    course_info = {}

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

    # Add course meeting time to description field
    if course_json["hours"] != "1" and course_json["hours"] != "":
        course_info["description"] += "\n\n" + \
            course_json["hours"] + " Yale College course credits"

    # Course title
    if len(course_json["title"]) > 32:
        course_info["title"] = course_json["title"][:30] + "..."
    else:
        course_info["title"] = course_json["title"]

    course_info["long_title"] = course_json["title"]

    # Course status
    course_info["extra_info"] = course_json["stat"]

    if course_info["extra_info"] == "C":
        course_info["title"] = "CANCELLED"
    elif course_info["extra_info"] == "F":
        course_info["title"] = "FULL:" + course_info["title"]

    # Instructors
    course_info["professors"] = professors_from_html(
        course_json["instructordetail_html"])

    # Codes

    course_info["course_codes"] = course_codes_from_fields(
        course_json["code"],
        course_json["xlist"],
        course_json["section"],
        course_json["crn"]
    )

    # Meeting times
    course_info["sessions"] = course_times_from_fields(
        course_json["meeting_html"],
        course_json["all_sections_remove_children"]
    )

    # Skills and areas
    course_info["skills"] = found_items(course_json["yc_attrs"],
                                        skills_map)
    course_info["areas"] = found_items(course_json["yc_attrs"],
                                       areas_map)

    # Final exam info
    course_info["exam"] = exam_from_field(course_json["final_exam"])

    # Additional attributes
    course_info["extra_flags"] = []

    if len(course_json["ci_attrs"]) > 0:
        course_info["extra_flags"].append(course_json["ci_attrs"])

    # Course homepage
    matched_homepage = re.findall(
        'href="([^"]*)"[^>]*>HOMEPAGE</a>', course_json["resources"])

    if len(matched_homepage) > 0:
        course_info["course_home_url"] = matched_homepage[0]

    # Link to syllabus
    matched_syllabus = re.findall(
        'href="([^"]*)"[^>]*>SYLLABUS</a>', course_json["resources"])

    if len(matched_syllabus) > 0:
        course_info["syllabus_url"] = matched_syllabus[0]

    return course_info

"""
Course fetching functions used by:
    /ferry/fetch_seasons.py
    /ferry/fetch_subjects.py
    /ferry/crawler/fetch_classes.py
"""

from typing import Any, Dict, List

import requests
import ujson


class FetchClassesError(Exception):
    """
    Object for class fetching exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


def fetch_previous_seasons():
    """
    Get list of seasons from previous CourseTable

    Returns
    -------
    seasons: list of seasons
    """
    middle_years = [str(x) for x in range(2014, 2020)]

    spring_seasons = [str(x) + "01" for x in middle_years]
    summer_seasons = [str(x) + "02" for x in middle_years]
    winter_seasons = [str(x) + "03" for x in middle_years]

    seasons = [
        *spring_seasons,
        *summer_seasons,
        *winter_seasons,
        "202001",
        "202002",
    ]

    return seasons


def fetch_season_subjects(season: str, api_key: str) -> List[str]:
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

    endpoint = "https://gw.its.yale.edu/soa-gateway/course/webservice/subjects"
    endpoint_args = f"?termCode={season}&mode=json&apiKey={api_key}"

    url = f"{endpoint}{endpoint_args}"

    req = requests.get(url)
    req.encoding = "utf-8"

    # Successful response
    if req.status_code == 200:

        subjects = ujson.loads(req.text)

        return subjects

    # Unsuccessful
    raise FetchClassesError(f"Unsuccessful response: code {req.status_code}")


def fetch_season_subject_courses(season: str, subject: str, api_key: str):
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

    endpoint = "https://gw.its.yale.edu/soa-gateway/course/webservice/index"
    endpoint_args = (
        f"?termCode={season}&subjectCode={subject}&mode=json&apiKey={api_key}"
    )
    url = f"{endpoint}{endpoint_args}"

    req = requests.get(url)
    req.encoding = "utf-8"

    # Successful response
    if req.status_code == 200:

        courses = ujson.loads(req.text)

        return courses

    # Unsuccessful
    raise FetchClassesError(f"Unsuccessful response: code {req.status_code}")


def fetch_season_courses(season: str, criteria: Dict[str, Any]) -> List[Dict[str, Any]]:
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

    payload = {"other": {"srcdb": season}, "criteria": criteria}

    req = requests.post(url, data=ujson.dumps(payload))
    req.encoding = "utf-8"

    # Successful response
    if req.status_code == 200:

        r_json = ujson.loads(req.text)

        if "fatal" in r_json.keys():
            raise FetchClassesError(f'Unsuccessful response: {r_json["fatal"]}')

        if "results" not in r_json.keys():
            raise FetchClassesError("Unsuccessful response: no results")

        return r_json["results"]

    # Unsuccessful
    raise FetchClassesError(f"Unsuccessful response: code {req.status_code}")


def fetch_previous_json(season: str, evals=False) -> List[Dict[str, Any]]:
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

    req = requests.get(url)
    req.encoding = "utf-8"

    # Successful response
    if req.status_code == 200:

        r_json = ujson.loads(req.text)

        return r_json

    # Unsuccessful
    raise FetchClassesError("Unsuccessful response: code {}".format(req.status_code))


def fetch_course_json(code: str, crn: str, srcdb: str) -> Dict[str, Any]:
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
        "matched": "crn:" + crn + "",
    }

    req = requests.post(url, data=ujson.dumps(payload))
    req.encoding = "utf-8"

    # Successful response
    if req.status_code == 200:

        course_json = ujson.loads(req.text)

        # exclude Yale's last-updated field (we use our own later on)
        if "last_updated" in course_json:
            del course_json["last_updated"]

        if "fatal" in course_json.keys():
            raise FetchClassesError(
                "Unsuccessful response: {}".format(course_json["fatal"])
            )

        return course_json

    # Unsuccessful
    raise FetchClassesError("Unsuccessful response: code {}".format(req.status_code))

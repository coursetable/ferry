"""
Course fetching functions used by:
    /ferry/fetch_seasons.py
    /ferry/fetch_subjects.py
    /ferry/crawler/fetch_classes.py
"""

from typing import Any

import ujson
from httpx import AsyncClient

from ferry.utils import request


class FetchClassesError(Exception):
    """
    Object for class fetching exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


async def fetch_season_courses_util(
    season: str,
    criteria: list[dict[str, Any]],
    client: AsyncClient = AsyncClient(timeout=None),
) -> list[dict[str, Any]]:
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

    req = await request(
        method="POST", client=client, url=url, data=ujson.dumps(payload)
    )
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


async def fetch_course_json(
    code: str, crn: str, srcdb: str, client: AsyncClient
) -> dict[str, Any]:
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

    # retry up to 10 times
    req = await request(
        method="POST",
        client=client,
        url=url,
        data=ujson.dumps(payload),
        attempts=10,
    )

    req.encoding = "utf-8"

    # Successful response
    if req.status_code == 200:
        course_json = ujson.loads(req.text)

        # exclude Yale's last-updated field (we use our own later on)
        if "last_updated" in course_json:
            del course_json["last_updated"]

        if "fatal" in course_json.keys():
            raise FetchClassesError(f"Unsuccessful response: {course_json['fatal']}")

        return course_json

    # Unsuccessful
    raise FetchClassesError("Unsuccessful response: code {req.status_code}")

from httpx import AsyncClient
from tqdm.asyncio import tqdm_asyncio
from pathlib import Path
import ujson
from typing import Any

from ferry.crawler.cache import load_cache_json, save_cache_json
from ferry.crawler.cas_request import request


# fetch overview info for all classes in each season
async def fetch_season_course_list(
    season: str,
    data_dir: Path,
    client: AsyncClient,
    fysem: bool = False,
    use_cache: bool = True,
) -> list[dict[str, Any]]:
    if fysem:
        criteria = [{"field": "fsem_attrs", "value": "Y"}]
        f_suffix = "_fysem"
    else:
        criteria = []
        f_suffix = ""

    # load from cache if it exists
    if (
        use_cache
        and (
            cache_load := load_cache_json(
                data_dir / "season_courses" / f"{season}{f_suffix}.json"
            )
        )
        is not None
    ):
        return cache_load

    url = "https://courses.yale.edu/api/?page=fose&route=search"

    payload = {"other": {"srcdb": season}, "criteria": criteria}

    req = await request(
        method="POST", client=client, url=url, data=ujson.dumps(payload)
    )
    req.encoding = "utf-8"

    # Unsuccessful response
    if req.status_code != 200:
        raise FetchClassesError(f"Unsuccessful response: code {req.status_code}")
    r_json = ujson.loads(req.text)

    if "fatal" in r_json.keys():
        raise FetchClassesError(f'Unsuccessful response: {r_json["fatal"]}')

    if "results" not in r_json.keys():
        raise FetchClassesError("Unsuccessful response: no results")

    season_courses = r_json["results"]
    save_cache_json(
        data_dir / "season_courses" / f"{season}{f_suffix}.json", season_courses
    )

    return season_courses


class FetchClassesError(Exception):
    """
    Object for class fetching exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


async def fetch_course_details(
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
        "group": f"code:{code}",
        "key": f"crn:{crn}",
        "srcdb": f"{srcdb}",
        "matched": f"crn:{crn}",
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

    # Unsuccessful response
    if req.status_code != 200:
        raise FetchClassesError(f"Unsuccessful response: code {req.status_code}")
    course_json = ujson.loads(req.text)

    # exclude Yale's last-updated field (we use our own later on)
    if "last_updated" in course_json:
        del course_json["last_updated"]

    if "fatal" in course_json:
        raise FetchClassesError(f"Unsuccessful response: {course_json['fatal']}")

    return course_json


# fetch detailed info for all classes in each season
async def fetch_all_season_courses_details(
    season: str,
    season_courses: list[dict[str, Any]],
    data_dir: Path,
    client: AsyncClient,
    use_cache: bool = True,
):
    # load from cache if it exists
    if (
        use_cache
        and (
            cache_load := load_cache_json(
                data_dir / "course_json_cache" / f"{season}.json"
            )
        )
        is not None
    ):
        return cache_load

    # merge all the JSON results per season
    futures = [
        fetch_course_details(
            course["code"], course["crn"], course["srcdb"], client=client
        )
        for course in season_courses
    ]
    aggregate_season_json = await tqdm_asyncio.gather(
        *futures, leave=False, desc=f"Fetching season {season}"
    )

    save_cache_json(
        data_dir / "course_json_cache" / f"{season}.json", aggregate_season_json
    )

    return aggregate_season_json

"""
This script fetches course evaluation data from the Yale
Online Course Evaluation (OCE), in JSON format through the
following steps:

    1. Selection of seasons to fetch ratings
    2. Construction of a cached check for Yale College courses
    3. Aggregation of all season courses into a queue
    4. Fetch and save OCE data for each course in the queue

"""

import asyncio
import traceback
from pathlib import Path
from urllib.parse import urlencode
from typing import cast

import diskcache
import ujson
from tqdm import tqdm

from ferry.crawler.cas_request import CASClient, request


class AuthError(Exception):
    pass


class FetchError(Exception):
    pass


async def is_yale_college(
    yale_college_cache: diskcache.Cache,
    season_code: str,
    crn: str,
    client: CASClient,
) -> bool:
    """
    Helper function to check if course is in Yale College
    (only Yale College and Summer Session courses are rated)
    """

    course_unique_id = f"{season_code}-{crn}"

    # Check cache
    if course_unique_id in yale_college_cache:
        return cast(bool, yale_college_cache.get(course_unique_id))

    all_params = {
        "other": {"srcdb": season_code},
        "criteria": [{"field": "crn", "value": crn}],
    }

    loop = asyncio.get_running_loop()

    all_response = await request(
        method="POST",
        client=client,
        url="https://courses.yale.edu/api/?page=fose&route=search",
        data=ujson.dumps(all_params),
    )

    if all_response is None:
        # We don't think this even exists, so just attempt it - truthy value.
        result = await loop.run_in_executor(
            None,
            yale_college_cache.set,
            course_unique_id,
            "try it anyways",
        )
        return result

    all_data = all_response.json()

    if all_data["count"] < 1:
        # We don't think this even e, so just attempt it - truthy value.
        result = await loop.run_in_executor(
            None,
            yale_college_cache.set,
            course_unique_id,
            "try it anyways",
        )
        return result

    yc_params = {
        "other": {"srcdb": season_code},
        "criteria": [
            {"field": "crn", "value": crn},
            {"field": "col", "value": "YC"},
        ],
    }

    yc_data = await request(
        method="POST",
        client=client,
        url="https://courses.yale.edu/api/?page=fose&route=search&col=YC",
        data=ujson.dumps(yc_params),
    )

    if yc_data is None:
        # We don't think this even exists, so just attempt it - truthy value.
        result = await loop.run_in_executor(
            None,
            yale_college_cache.set,
            course_unique_id,
            "try it anyways",
        )
        return result

    yc_data = yc_data.json()

    if yc_data["count"] == 0:
        # Not available in Yale College.
        result = await loop.run_in_executor(
            None, yale_college_cache.set, course_unique_id, False
        )
        return result

    result = await loop.run_in_executor(
        None, yale_college_cache.set, course_unique_id, True
    )
    return result


async def fetch_course_evals(
    season_code: str,
    crn: str,
    data_dir: Path,
    client: CASClient,
    yale_college_cache: diskcache.Cache,
) -> tuple[bytes | None, str, str, Path]:
    course_unique_id = f"{season_code}-{crn}"

    output_path = data_dir / "parsed_evaluations" / f"{course_unique_id}.json"

    if output_path.is_file():
        # tqdm.write(f"Skipping course {course_unique_id} - already exists")
        # The JSON will be loaded at the process ratings step
        return None, crn, season_code, output_path

    if not await is_yale_college(
        season_code=season_code,
        crn=crn,
        client=client,
        yale_college_cache=yale_college_cache,
    ):
        # tqdm.write(f"Skipping course {course_unique_id} - not in yale college")
        return None, crn, season_code, output_path

    # tqdm.write(f"Fetching {course_unique_id} ... ", end="")
    try:
        eval_page = await fetch_eval_page(
            client=client,
            crn=crn,
            season_code=season_code,
            data_dir=data_dir,
        )  # this is the raw html request, must be processed
        return eval_page, crn, season_code, output_path
        # tqdm.write("dumped in JSON")
    except FetchError as error:
        # tqdm.write(f"skipped {course_unique_id}: {error}")
        pass
    except AuthError as error:
        raise SystemExit(error)
    except Exception as error:
        traceback.print_exc()
        tqdm.write(f"skipped {course_unique_id}: unknown error {error}")

    return None, crn, season_code, output_path


async def fetch_eval_page(
    client: CASClient, crn: str, season_code: str, data_dir: Path
) -> bytes:
    """
    Gets evaluation data and comments for the specified course in specified term.

    Parameters
    ----------
    client:
        The current session client with login cookie.
    crn:
        CRN of this course.
    season:
        term code of this course.
    data_dir:
        Path to data directory.

    Returns
    -------
    course_eval:
        Dictionary with all evaluation data.
    """
    questions_index = data_dir / "rating_cache" / "questions_index"
    html_file = questions_index / f"{season_code}_{crn}.html"
    if html_file.is_file():
        with open(html_file, "rb") as file:
            return file.read()

    # OCE website for evaluations
    url_eval = f"https://oce.app.yale.edu/ocedashboard/studentViewer/courseSummary?{urlencode({'crn': crn, 'termCode': season_code})}"
    payload = {
        "cookie": client.cas_cookie,
        "url": url_eval,
    }
    try:
        page_index = await request(
            method="POST",
            url=client.url,
            client=client,
            json=payload,
        )
    except Exception as err:
        raise FetchError(f"Error fetching evaluations for {season_code}-{crn}: {err}")

    if "Central Authentication Service" in page_index.text:
        raise AuthError(f"Cookie auth failed for {season_code}-{crn}")

    if page_index.status_code == 500:
        raise FetchError(f"Evaluations for term {season_code}-{crn} are unavailable")

    if page_index.status_code != 200:  # Evaluation data for this term not available
        raise FetchError(f"Error fetching evaluations for {season_code}-{crn}")

    # save raw HTML in case we ever need it
    questions_index.mkdir(parents=True, exist_ok=True)
    with open(html_file, "wb") as file:
        file.write(page_index.content)

    return page_index.content

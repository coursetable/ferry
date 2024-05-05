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
import concurrent.futures
import traceback
from pathlib import Path
from urllib.parse import urlencode
from typing import cast

import diskcache
import ujson
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from .to_table import create_rating_tables
from .parse import (
    parse_eval_page,
    PageIndex,
)
from ferry.crawler.classes.parse import ParsedCourse
from ferry.crawler.cas_request import create_client, CASClient, request
from ferry.crawler.cache import load_cache_json


class AuthError(Exception):
    """
    Object for auth exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class FetchError(Exception):
    """
    Error object for fetch ratings exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


EXCLUDE_SEASONS_BEFORE = (
    "202101"  # exclude seasons before and including this because no evaluations
)


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


# --------------------------------------------------------
# Load all seasons and compare with selection if specified
# --------------------------------------------------------


async def fetch_ratings(
    cas_cookie: str,
    seasons: list[str],
    data_dir: Path,
    courses: dict[str, list[ParsedCourse]] | None = None,
):
    # -----------------------------------
    # Queue courses to query from seasons
    # -----------------------------------

    # Test cases----------------------------------------------------------------
    # queue = [
    #     ("201903", "11970"),  # basic test
    #     ("201703", "10738"),  # no evaluations available
    #     ("201703", "13958"),  # DRAM class?
    #     ("201703", "10421"),  # APHY 990 (class not in Yale College)
    #     ("201703", "16119"),  # no evaluations available (doesn't show in OCE)
    #     ("201802", "30348"),  # summer session course
    # ]
    # --------------------------------------------------------------------------

    # predetermine all valid seasons
    seasons = list(
        filter(
            lambda season: int(season) > int(EXCLUDE_SEASONS_BEFORE),
            seasons,
        )
    )

    # Status update
    print(f"Fetching course ratings for valid seasons: {seasons}...")

    # initiate Yale client session to access ratings
    client = create_client(cas_cookie=cas_cookie)

    # Season level is synchronous, following same logic as fetch_classes.py
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for season in (pbar := tqdm(seasons, desc="Season Progress", leave=False)):
            pbar.set_postfix({"season": season})

            if (
                season_courses := (
                    load_cache_json(data_dir / "season_courses" / f"{season}.json")
                    if courses is None
                    else courses[season]
                )
            ) is None:
                raise FetchError(
                    f"Season {season} not found in season_courses directory."
                )

            # Test for first 100 courses
            season_courses = season_courses[:100]
            yale_college_cache = diskcache.Cache(data_dir / "yale_college_cache")

            futures = [
                fetch_course_evals(
                    season_code=season,
                    crn=course["crn"],
                    data_dir=data_dir,
                    client=client,
                    yale_college_cache=yale_college_cache,
                )
                for course in season_courses
            ]

            # Chunking is necessary as the lambda proxy function has a concurrency limit of 10.
            chunk_size = client.chunk_size
            raw_course_evals: list[tuple[PageIndex | None, str, str, Path]] = []

            for chunk_begin in tqdm(
                range(0, len(season_courses), chunk_size),
                leave=False,
                desc=f"Fetching ratings",
            ):
                chunk = await tqdm_asyncio.gather(
                    *futures[chunk_begin : chunk_begin + chunk_size],
                    leave=False,
                    desc=f"Chunk {int(chunk_begin / chunk_size)}",
                )
                raw_course_evals.extend(chunk)

            # It's not exactly necessary to make the parallelized processing async here because of the season-level sync loop.
            # However, if the season-loop sync loop becomes async, this will be non-blocking.
            loop = asyncio.get_running_loop()
            futures = [
                loop.run_in_executor(executor, parse_eval_page, *args)
                for args in raw_course_evals
            ]
            await tqdm_asyncio.gather(*futures, leave=False, desc=f"Processing ratings")

    await create_rating_tables(data_dir=data_dir)

    await client.aclose()

    print("\033[F", end="")
    print(f"Fetching course ratings for valid seasons: {seasons}... ✔")


async def fetch_course_evals(
    season_code: str,
    crn: str,
    data_dir: Path,
    client: CASClient,
    yale_college_cache: diskcache.Cache,
) -> tuple[PageIndex | None, str, str, Path]:
    course_unique_id = f"{season_code}-{crn}"

    output_path = data_dir / "course_evals" / f"{course_unique_id}.json"

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
    # pylint: disable=broad-except
    except Exception as error:
        traceback.print_exc()
        tqdm.write(f"skipped {course_unique_id}: unknown error {error}")

    return None, crn, season_code, output_path


async def fetch_eval_page(
    client: CASClient, crn: str, season_code: str, data_dir: Path
) -> PageIndex:
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
        return PageIndex(html_file)

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

    return PageIndex(page_index)

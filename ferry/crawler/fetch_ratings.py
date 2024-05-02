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
from typing import Any

import diskcache
import ujson
from httpx import AsyncClient
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from ferry.includes.cas import create_client
from ferry.includes.rating_parsing import parse_ratings
from ferry.includes.rating_processing import (
    AuthError,
    CrawlerError,
    fetch_course_eval,
    process_course_eval,
)
from ferry.utils import load_cache_json, request


class FetchRatingsError(Exception):
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
    client: AsyncClient = AsyncClient(timeout=None),
) -> str | bool:
    """
    Helper function to check if course is in Yale College
    (only Yale College and Summer Session courses are rated)
    """

    course_unique_id = f"{season_code}-{crn}"

    # Check cache
    if course_unique_id in yale_college_cache:
        return yale_college_cache.get(course_unique_id)

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
    courses: dict[str, Any] | None = None,
):
    yale_college_cache = diskcache.Cache(data_dir / "yale_college_cache")

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
    client = create_client(
        cas_cookie=cas_cookie,
    )

    # Season level is synchronous, following same logic as fetch_classes.py
    with concurrent.futures.ProcessPoolExecutor() as executor:
        for season in (pbar := tqdm(seasons, desc="Season Progress", leave=False)):
            pbar.set_postfix({"season": season})

            if (
                season_courses := load_cache_json(
                    data_dir / "season_courses" / f"{season}.json"
                )
                if courses is None
                else courses[season]
            ) is None:
                raise FetchRatingsError(
                    f"Season {season} not found in season_courses directory."
                )

            # Test for first 100 courses
            season_courses = season_courses[:100]

            # Check if course is in Yale College
            futures = [
                is_yale_college(
                    season_code=season,
                    crn=course["crn"],
                    client=client,
                    yale_college_cache=yale_college_cache,
                )
                for course in season_courses
            ]
            is_yale_college_results = await tqdm_asyncio.gather(
                *futures, leave=False, desc=f"Checking Yale College"
            )

            futures = [
                fetch_course_ratings(
                    season_code=season,
                    crn=course["crn"],
                    data_dir=data_dir,
                    client=client,
                    is_yale_college=is_yale_college_results[idx],
                )
                for idx, course in enumerate(season_courses)
            ]

            # Chunking is necessary as the lambda proxy function has a concurrency limit of 10.
            chunk_size = client.chunk_size
            raw_course_evals = []

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
                loop.run_in_executor(executor, process_course_eval, *args)
                for args in raw_course_evals
            ]
            await tqdm_asyncio.gather(*futures, leave=False, desc=f"Processing ratings")

    # Parse course ratings, parallelized similar to process_course_eval
    await parse_ratings(data_dir=data_dir)

    await client.aclose()

    print("\033[F", end="")
    print(f"Fetching course ratings for valid seasons: {seasons}... ✔")


async def fetch_course_ratings(
    season_code: str,
    crn: str,
    data_dir: Path,
    client: AsyncClient,
    is_yale_college: bool,
):
    course_unique_id = f"{season_code}-{crn}"

    output_path = data_dir / "course_evals" / f"{course_unique_id}.json"

    if output_path.is_file():
        # tqdm.write(f"Skipping course {course_unique_id} - already exists")
        # The JSON will be loaded at the process ratings step
        return None, None, None, output_path

    if not is_yale_college:
        # tqdm.write(f"Skipping course {course_unique_id} - not in yale college")
        return None, None, None, None

    # tqdm.write(f"Fetching {course_unique_id} ... ", end="")
    try:
        course_eval = await fetch_course_eval(
            client=client,
            crn_code=crn,
            term_code=season_code,
            data_dir=data_dir,
        )  # this is the raw html request, must be processed

        return course_eval, crn, season_code, output_path
        # tqdm.write("dumped in JSON")
    except CrawlerError as error:
        # tqdm.write(f"skipped {course_unique_id}: {error}")
        pass
    except AuthError as error:
        raise SystemExit(error)
    # pylint: disable=broad-except
    except Exception as error:
        traceback.print_exc()
        tqdm.write(f"skipped {course_unique_id}: unknown error {error}")

    return None, None, None, None


# testing function
if __name__ == "__main__":
    import asyncio

    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())

    seasons = ujson.load(open("data/course_seasons.json", "r"))

    class args:
        pass

    from ferry.utils import init_cas

    init_cas(args)

    asyncio.run(
        fetch_ratings(
            seasons=seasons,
            data_dir=Path("data"),
            cas_cookie=args.cas_cookie,
        )
    )

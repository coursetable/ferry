"""
Fetches the following information from the Yale Courses API:

    (1) A list of all courses for each season
        (/api_output/season_courses/)

    (2) Detailed information for each course, for each season
        (/api_output/course_json_cache/)
"""
from httpx import AsyncClient
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
from pathlib import Path

from ferry.includes.class_parsing import extract_course_info
from ferry.includes.class_processing import fetch_course_json, fetch_season_courses_util
from ferry.utils import load_cache_json, save_cache_json

# -----------------------------------------
# Retrieve courses from unofficial Yale API
# -----------------------------------------


async def fetch_classes(
    seasons: list[str],
    data_dir: Path,
    client: AsyncClient = AsyncClient(timeout=None),
    use_cache: bool = True,
) -> dict:
    # Concurrency with async at the season level overloads the CPU
    # futures = [ fetch_class(season, data_dir=data_dir, client=client) for season in seasons ]
    # classes = await tqdm_asyncio.gather(*futures, desc="Season Progress")

    print(f"Fetching course info for seasons: {seasons}...")

    classes = {}
    for season in tqdm(seasons, desc="Season Progress", leave=False):
        classes[season] = await fetch_class(season, data_dir=data_dir, client=client, use_cache=use_cache)

    print("\033[F", end="")
    print(f"Fetching course info for seasons: {seasons}... ✔")

    return classes


# -----------------------------------------
# Fetch Utility Functions
# -----------------------------------------


async def fetch_class(season: str, data_dir: Path, client: AsyncClient, use_cache: bool = True):
    # fetch season classes
    season_courses = await fetch_season_courses(
        season, data_dir=data_dir, client=client, use_cache=use_cache
    )
    season_fysem_courses = await fetch_season_courses(
        season, data_dir=data_dir, client=client, fysem=True, use_cache=use_cache
    )

    # fetch detailed info for all classes in each season
    aggregate_season_json = await fetch_aggregate_season_json(
        season, season_courses, data_dir=data_dir, client=client, use_cache=use_cache
    )

    # parse courses
    parsed_courses = parse_courses(
        season, aggregate_season_json, season_fysem_courses, data_dir=data_dir, use_cache=use_cache
    )

    return parsed_courses


# fetch overview info for all classes in each season
async def fetch_season_courses(
    season: str, data_dir: Path, client: AsyncClient, fysem: bool = False, use_cache: bool = True
):
    if fysem:
        criteria = [{"field": "fsem_attrs", "value": "Y"}]
        f_suffix = "_fysem"
    else:
        criteria = []
        f_suffix = ""

    # load from cache if it exists
    if use_cache and (
        cache_load := load_cache_json(
            data_dir / "season_courses" / f"{season}{f_suffix}.json"
        )
    ) is not None:
        return cache_load

    season_courses = await fetch_season_courses_util(
        season=season, criteria=criteria, client=client
    )

    save_cache_json(
        data_dir / "season_courses" / f"{season}{f_suffix}.json", season_courses
    )

    return season_courses


# fetch detailed info for all classes in each season
async def fetch_aggregate_season_json(
    season: str, season_courses, data_dir: Path, client: AsyncClient, use_cache: bool = True
):
    # load from cache if it exists

    if use_cache and (
        cache_load := load_cache_json(data_dir / "course_json_cache" / f"{season}.json")
    ) is not None:
        return cache_load

    # merge all the JSON results per season
    course_futures = [
        fetch_course_json(course["code"], course["crn"], course["srcdb"], client=client)
        for course in season_courses
    ]
    aggregate_season_json = await tqdm_asyncio.gather(
        *course_futures, leave=False, desc=f"Fetching season {season}"
    )

    save_cache_json(
        data_dir / "course_json_cache" / f"{season}.json", aggregate_season_json
    )

    return aggregate_season_json


# combine regular and fysem courses in each season
def parse_courses(season: str, aggregate_season_json, fysem_courses, data_dir: Path, use_cache: bool = True):
    # load from cache if it exists
    if use_cache and (
        cache_load := load_cache_json(data_dir / "parsed_courses" / f"{season}.json")
    ) is not None:
        return cache_load

    # parse course JSON in season
    parsed_course_info = []
    # not worth parallelizing, already pretty quick
    for x in tqdm(aggregate_season_json, leave=False, desc=f"Parsing season {season}"):
        try:
            parsed_course_info.append(extract_course_info(x, season, fysem_courses))
        except Exception as e:
            print(f"Error parsing course {x['code']} in season {season}: {e}")

    save_cache_json(data_dir / "parsed_courses" / f"{season}.json", parsed_course_info)

    return parsed_course_info

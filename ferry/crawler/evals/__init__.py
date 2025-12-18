import concurrent.futures
from pathlib import Path

import diskcache
import httpx
from tqdm import tqdm

from ferry.crawler.cache import load_cache_json, save_cache_json
from ferry.crawler.cas_request import CASClient, USER_AGENT
from ferry.crawler.classes.parse import ParsedCourse

from .fetch import FetchError, fetch_course_evals
from .parse import parse_eval_page

# exclude seasons before and including this because no evaluations
EXCLUDE_SEASONS_BEFORE = "202101"


async def crawl_evals(
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
    print(f"Fetching course evals for valid seasons: {seasons}...")

    # initiate Yale client session to access evals
    client = httpx.AsyncClient(headers={"User-Agent": USER_AGENT})
    cas_client = CASClient(cas_cookie=cas_cookie)

    # Season level is synchronous, following same logic as class fetcher
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

            yale_college_cache = diskcache.Cache(data_dir / "yale_college_cache")
            raw_course_evals: list[tuple[bytes | None, str]] = []

            for course in tqdm(season_courses, desc="Course Progress", leave=False):
                raw_course_evals.append(
                    await fetch_course_evals(
                        season_code=season,
                        crn=course["crn"],
                        data_dir=data_dir,
                        client=client,
                        cas_client=cas_client,
                        yale_college_cache=yale_college_cache,
                    )
                )

            data = []
            for page, crn in raw_course_evals:
                parsed = parse_eval_page(page, crn, season)
                if parsed is None:
                    continue
                data.append(parsed)
            data.sort(key=lambda x: x["crn"])
            save_cache_json(data_dir / "parsed_evaluations" / f"{season}.json", data)

    await client.aclose()
    print("\033[F", end="")
    print(f"Fetching course evals for valid seasons: {seasons}... ✔")

from datetime import date
from pathlib import Path

import httpx
from tqdm import tqdm

from ferry.crawler.cache import load_cache_json, save_cache_json
from ferry.crawler.cas_request import USER_AGENT
from ferry.crawler.classes.parse import ParsedCourse

from .fetch import FetchError, fetch_all_season_demand_pages


async def crawl_demand(
    cas_cookie: str,
    seasons: list[str],
    data_dir: Path,
    courses: dict[str, list[ParsedCourse]] | None = None,
    use_cache: bool = True,
):
    print(f"Fetching course demand statistics for seasons: {seasons}...")

    # Captured once so an entire run uses a consistent "today" for resolving
    # the site's bare MM/DD dates into real years, even if the crawl spans
    # midnight.
    reference_date = date.today()

    async with httpx.AsyncClient(
        headers={"Cookie": cas_cookie, "User-Agent": USER_AGENT},
        timeout=30.0,
    ) as client:
        for season in (pbar := tqdm(seasons, desc="Season Progress", leave=False)):
            pbar.set_postfix({"season": season})

            if (
                season_courses := (
                    courses[season]
                    if courses is not None
                    else load_cache_json(data_dir / "parsed_courses" / f"{season}.json")
                )
            ) is None:
                raise FetchError(
                    f"Season {season} not found in parsed_courses directory. Run --crawl-classes first."
                )

            # Dedup by (subject, number) for different sections of the same course
            seen: set[tuple[str, str]] = set()
            unique_courses: list[tuple[str, str]] = []
            for course in season_courses:
                key = (course["subject"], course["number"])
                if key not in seen:
                    seen.add(key)
                    unique_courses.append(key)

            season_demand = await fetch_all_season_demand_pages(
                season,
                unique_courses,
                client,
                data_dir,
                reference_date,
                use_cache=use_cache,
            )

            save_cache_json(data_dir / "parsed_demand" / f"{season}.json", season_demand)

    print("\033[F", end="")
    print(f"Fetching course demand statistics for seasons: {seasons}... ✔")

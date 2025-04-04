from pathlib import Path
from httpx import AsyncClient
from tqdm import tqdm

from .fetch import (
    fetch_season_course_list,
    fetch_all_season_courses_details,
    fetch_cws_api,
)
from .parse import parse_courses, ParsedCourse


async def crawl_classes(
    seasons: list[str],
    data_dir: Path,
    client: AsyncClient,
    cws_api_key: str,
    use_cache: bool = True,
) -> dict[str, list[ParsedCourse]]:
    # Concurrency with async at the season level overloads the CPU
    # futures = [ fetch_class(season, data_dir=data_dir, client=client) for season in seasons ]
    # classes = await tqdm_asyncio.gather(*futures, desc="Season Progress")

    print(f"Fetching course info for seasons: {seasons}...")

    classes: dict[str, list[ParsedCourse]] = {}
    for season in tqdm(seasons, desc="Season Progress", leave=False):
        season_courses = await fetch_season_course_list(
            season, data_dir=data_dir, client=client, use_cache=use_cache
        )
        season_fysem_courses = await fetch_season_course_list(
            season,
            data_dir=data_dir,
            client=client,
            fysem=True,
            use_cache=use_cache,
        )

        aggregate_season_json = await fetch_all_season_courses_details(
            season,
            season_courses,
            data_dir=data_dir,
            client=client,
            use_cache=use_cache,
        )

        cws_season_json = []
        try:
            cws_season_json = await fetch_cws_api(
                season,
                season_courses,
                data_dir=data_dir,
                cws_api_key=cws_api_key,
                client=client,
                use_cache=use_cache,
            )
        except:
            print(f"Failed to fetch CWS API for {season}.\n Continuing...")
            pass

        classes[season] = parse_courses(
            season,
            aggregate_season_json,
            cws_season_json,
            set(x["crn"] for x in season_fysem_courses),
            data_dir=data_dir,
            use_cache=use_cache,
        )

    print("\033[F", end="")
    print(f"Fetching course info for seasons: {seasons}... ✔")

    return classes

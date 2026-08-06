from pathlib import Path

from tqdm import tqdm

from ferry.crawler.cache import load_cache_json, save_cache_json
from ferry.crawler.cas_request import CASClient
from ferry.crawler.classes.parse import ParsedCourse

from .fetch import AuthError, FetchError, fetch_course_demand_page
from .parse import EmptyDemandError, parse_course_demand_page


async def crawl_demand(
    cas_cookie: str,
    seasons: list[str],
    data_dir: Path,
    courses: dict[str, list[ParsedCourse]] | None = None,
    use_cache: bool = True,
):
    print(f"Fetching course demand statistics for seasons: {seasons}...")

    cas_client = CASClient(cas_cookie=cas_cookie)

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

        season_demand = []

        # TODO: Handle cross-listed courses
        # rn, it's fetching the same page for each course in a cross listing
        seen: set[tuple[str, str]] = set()
        for course in tqdm(season_courses, desc="Course Progress", leave=False):
            subject_code, course_number = course["subject"], course["number"]
            if (subject_code, course_number) in seen:
                continue
            seen.add((subject_code, course_number))

            try:
                page = fetch_course_demand_page(
                    term_code=season,
                    subject_code=subject_code,
                    course_number=course_number,
                    client=cas_client,
                    data_dir=data_dir,
                    use_cache=use_cache,
                )
            except FetchError as error:
                tqdm.write(f"skipped {season}-{subject_code}{course_number}: {error}")
                continue
            except AuthError as error:
                raise SystemExit(error)

            try:
                parsed = parse_course_demand_page(
                    page, season, subject_code, course_number
                )
            except EmptyDemandError:
                continue
            if parsed is not None:
                season_demand.append(parsed)

        save_cache_json(data_dir / "parsed_demand" / f"{season}.json", season_demand)

    print("\033[F", end="")
    print(f"Fetching course demand statistics for seasons: {seasons}... ✔")

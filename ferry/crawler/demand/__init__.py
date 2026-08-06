import asyncio
from pathlib import Path

from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

from ferry.crawler.cache import load_cache_json, save_cache_json
from ferry.crawler.cas_request import CASClient
from ferry.crawler.classes.parse import ParsedCourse

from .fetch import AuthError, FetchError, fetch_course_demand_page
from .parse import EmptyDemandError, ParsedCourseDemand, parse_course_demand_page

# No clue what upper limit is on this. 8 is done in ~6mins
# Claude says to keep concurrency modest to avoid overloading
# the session or tripping potential rate limits/firewall
# but also i doubt yale put any measures like that on this site so...
MAX_CONCURRENT_REQUESTS = 8


async def fetch_and_parse_course(
    season: str,
    subject_code: str,
    course_number: str,
    cas_client: CASClient,
    data_dir: Path,
    use_cache: bool,
    semaphore: asyncio.Semaphore,
) -> ParsedCourseDemand | None:
    async with semaphore:
        try:
            page = await asyncio.to_thread(
                fetch_course_demand_page,
                term_code=season,
                subject_code=subject_code,
                course_number=course_number,
                client=cas_client,
                data_dir=data_dir,
                use_cache=use_cache,
            )
        except FetchError as error:
            tqdm.write(f"skipped {season}-{subject_code}{course_number}: {error}")
            return None
        except AuthError as error:
            raise SystemExit(error)

    try:
        return parse_course_demand_page(page, season, subject_code, course_number)
    except EmptyDemandError:
        return None
    except Exception as error:
        tqdm.write(
            f"error parsing {season}-{subject_code}{course_number}: {error}"
        )
        return None


async def crawl_demand(
    cas_cookie: str,
    seasons: list[str],
    data_dir: Path,
    courses: dict[str, list[ParsedCourse]] | None = None,
    use_cache: bool = True,
):
    print(f"Fetching course demand statistics for seasons: {seasons}...")

    cas_client = CASClient(cas_cookie=cas_cookie)
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

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

        # TODO: Handle cross-listed courses
        # rn, it's fetching the same page for each course in a cross listing
        seen: set[tuple[str, str]] = set()
        unique_courses: list[tuple[str, str]] = []
        for course in season_courses:
            key = (course["subject"], course["number"])
            if key not in seen:
                seen.add(key)
                unique_courses.append(key)

        futures = [
            fetch_and_parse_course(
                season,
                subject_code,
                course_number,
                cas_client,
                data_dir,
                use_cache,
                semaphore,
            )
            for subject_code, course_number in unique_courses
        ]
        results = await tqdm_asyncio.gather(
            *futures, leave=False, desc=f"Fetching demand for {season}"
        )

        season_demand = [parsed for parsed in results if parsed is not None]
        save_cache_json(data_dir / "parsed_demand" / f"{season}.json", season_demand)

    print("\033[F", end="")
    print(f"Fetching course demand statistics for seasons: {seasons}... ✔")

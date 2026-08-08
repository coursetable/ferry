# Fetches Yale course demand statistics detail pages from
# ivy.yale.edu/course-stats/course/courseDetail.
# Requires a CAS-authenticaed session cookie, sent as a plain Cookie header -
# unlike evals' oce.app.yale.edu, this site works fine with a native
# httpx.AsyncClient, no curl subprocess needed.
#
# One page per (term, subject, course number)
# contains registered, waitlisted, visiting counts per day

import asyncio
from pathlib import Path

import httpx
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio

COURSE_DETAIL_URL = "https://ivy.yale.edu/course-stats/course/courseDetail"

# No clue what upper limit is on this. 1 is ~20 mins
# ngl anything above 8 seems to be the same ~10 mins
# Claude says to keep concurrency modest to avoid overloading
# the session or tripping potential rate limits/firewall
MAX_CONCURRENT_REQUESTS = 4

class AuthError(Exception):
    pass

class FetchError(Exception):
    pass

async def fetch_course_demand_page(
    term_code: str,
    subject_code: str,
    course_number: str,
    client: httpx.AsyncClient,
    data_dir: Path,
    num_days: int = 7,
    use_cache: bool = True,
) -> bytes:
    """
    Downloads the course demand statistics detail page for a single course.
    term_code: Season/term code, e.g. "202603".
    subject_code: Subject code, e.g. "CENG".
    course_number: Catalog course number, e.g. "1200".
    client: httpx.AsyncClient with the CAS session Cookie header set.
    data_dir: Path to data directory.
    num_days: Number of trailing days of data to request.
    returns: Raw HTML page contents.
    """
    course_unique_id = f"{term_code}_{subject_code}_{course_number}"
    output_path = data_dir / "demand_cache" / f"{course_unique_id}.html"

    if use_cache and output_path.is_file():
        return output_path.read_bytes()

    try:
        response = await client.get(
            COURSE_DETAIL_URL,
            params={
                "termCode": term_code,
                "subjectCode": subject_code,
                "courseNumber": course_number,
                "numDays": num_days,
            },
        )
    except Exception as err:
        raise FetchError(f"Error fetching demand page for {course_unique_id}: {err}")

    # An expired/invalid session gets redirected to CAS login rather than
    # returning the page directly.
    if response.status_code in (301, 302, 303, 307, 308):
        raise AuthError(f"Cookie auth failed for {course_unique_id}")

    if response.status_code != 200:
        raise FetchError(
            f"Unsuccessful response for {course_unique_id}: code {response.status_code}"
        )

    page = response.content
    if b"Central Authentication Service" in page:
        raise AuthError(f"Cookie auth failed for {course_unique_id}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(page)

    return page


# fetch demand pages for all courses in a season, at bounded concurrency
async def fetch_all_season_demand_pages(
    season: str,
    unique_courses: list[tuple[str, str]],
    client: httpx.AsyncClient,
    data_dir: Path,
    use_cache: bool = True,
) -> list[tuple[str, str, bytes | None]]:
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)

    async def fetch_one(
        subject_code: str, course_number: str
    ) -> tuple[str, str, bytes | None]:
        async with semaphore:
            try:
                page = await fetch_course_demand_page(
                    term_code=season,
                    subject_code=subject_code,
                    course_number=course_number,
                    client=client,
                    data_dir=data_dir,
                    use_cache=use_cache,
                )
            except FetchError as error:
                tqdm.write(f"skipped {season}-{subject_code}{course_number}: {error}")
                return subject_code, course_number, None
            except AuthError:
                # Let this propagate as a normal exception rather than
                # SystemExit - asyncio's gather/as_completed only reliably
                # surfaces Exception subclasses from concurrent tasks.
                # SystemExit (a BaseException) tends to get dropped with a
                # "Task exception was never retrieved" warning instead of a
                # clean error when multiple tasks fail around the same time.
                raise
        return subject_code, course_number, page

    futures = [
        fetch_one(subject_code, course_number)
        for subject_code, course_number in unique_courses
    ]
    return await tqdm_asyncio.gather(
        *futures, leave=False, desc=f"Fetching demand for {season}"
    )

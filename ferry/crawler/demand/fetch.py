# Fetches Yale course demand statistics detail pages from
# ivy.yale.edu/course-stats/course/courseDetail.
# Requires a CAS-authenticaed session cookie.
#
# One page per (term, subject, course number)
# contains registered, waitlisted, visiting counts per day

from pathlib import Path

from ferry.crawler.cas_request import CASClient, sync_request

COURSE_DETAIL_URL = "https://ivy.yale.edu/course-stats/course/courseDetail"

class AuthError(Exception):
    pass

class FetchError(Exception):
    pass

def fetch_course_demand_page(
    term_code: str,
    subject_code: str,
    course_number: str,
    client: CASClient,
    data_dir: Path,
    num_days: int = 7,
    use_cache: bool = True,
) -> bytes:
    """
    Downloads the course demand statistics detail page for a single course.
    term_code: Season/term code, e.g. "202603".
    subject_code: Subject code, e.g. "CENG".
    course_number: Catalog course number, e.g. "1200".
    client: CAS-authenticated session client.
    data_dir: Path to data directory.
    num_days: Number of trailing days of data to request.
    returns: Raw HTML page contents.
    """
    course_unique_id = f"{term_code}_{subject_code}_{course_number}"
    output_path = data_dir / "demand_cache" / f"{course_unique_id}.html"

    if use_cache and output_path.is_file():
        return output_path.read_bytes()

    url = (
        f"{COURSE_DETAIL_URL}?termCode={term_code}&subjectCode={subject_code}"
        f"&courseNumber={course_number}&numDays={num_days}"
    )

    try:
        page = sync_request(url=url, client=client)
    except Exception as err:
        raise FetchError(f"Error fetching demand page for {course_unique_id}: {err}")

    if "Central Authentication Service" in str(page):
        raise AuthError(f"Cookie auth failed for {course_unique_id}")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_bytes(page)

    return page

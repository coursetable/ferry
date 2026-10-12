import asyncio
import datetime
from pathlib import Path

import httpx
from httpx import AsyncClient

from ferry.crawler.cache import save_cache_json

from .parse import ParsedAcademicCalendar, parse_academic_calendar

ACADEMIC_CALENDAR_URL = "https://catalog.yale.edu/ycps/calendar/"
ACADEMIC_CALENDAR_ARCHIVE_URL = (
    "https://catalog.yale.edu/archive/{academic_year}/ycps/calendar/"
)
# The catalog archive starts with the 2013-2014 edition
ARCHIVE_FIRST_YEAR = 2013
DEFAULT_MAX_RETRIES = 3


async def fetch_with_retries(
    client: AsyncClient, url: str, max_retries: int
) -> httpx.Response:
    """
    GET a page, retrying with exponential backoff when the connection fails or
    the server returns a 5xx. catalog.yale.edu occasionally drops connections
    without responding.
    """
    for attempt in range(max_retries + 1):
        try:
            r = await client.get(url, follow_redirects=True)
            if r.status_code < 500 or attempt == max_retries:
                return r
            reason = f"code {r.status_code}"
        except httpx.TransportError as e:
            if attempt == max_retries:
                raise
            reason = repr(e)
        delay = 2**attempt
        print(f"(retrying in {delay}s after {reason})", end=" ")
        await asyncio.sleep(delay)
    raise AssertionError("unreachable")


async def crawl_academic_calendar(
    data_dir: Path, client: AsyncClient, max_retries: int = DEFAULT_MAX_RETRIES
) -> ParsedAcademicCalendar:
    """
    Fetch the current Yale College academic calendar and save it to
    `academic_calendar/<academic_year>.json`. Previous years' files are kept, so
    the directory accumulates one file per academic year.
    """
    print("Fetching academic calendar...", end=" ")
    r = await fetch_with_retries(client, ACADEMIC_CALENDAR_URL, max_retries)
    if r.status_code != 200:
        print("✘")
        raise SystemExit(
            f"Unsuccessful academic calendar response: code {r.status_code}"
        )

    calendar = parse_academic_calendar(r.text, source_url=ACADEMIC_CALENDAR_URL)
    save_cache_json(
        data_dir / "academic_calendar" / f"{calendar['academic_year']}.json",
        calendar,
    )
    print("✔")
    print(f"Parsed {len(calendar['events'])} events for {calendar['academic_year']}")
    return calendar


async def crawl_academic_calendar_archive(
    data_dir: Path, client: AsyncClient, max_retries: int = DEFAULT_MAX_RETRIES
) -> list[ParsedAcademicCalendar]:
    """
    Fetch every archived Yale College academic calendar from the catalog
    archive and save each to `academic_calendar/<academic_year>.json`.
    """
    calendars: list[ParsedAcademicCalendar] = []
    for year in range(ARCHIVE_FIRST_YEAR, datetime.date.today().year):
        url = ACADEMIC_CALENDAR_ARCHIVE_URL.format(academic_year=f"{year}-{year + 1}")
        print(f"Fetching {year}-{year + 1} academic calendar...", end=" ")
        r = await fetch_with_retries(client, url, max_retries)
        # The most recent year is only archived once the next catalog is out
        if r.status_code == 404:
            print("not archived")
            continue
        if r.status_code != 200:
            print("✘")
            raise SystemExit(
                f"Unsuccessful academic calendar response: code {r.status_code}"
            )

        calendar = parse_academic_calendar(r.text, source_url=url)
        save_cache_json(
            data_dir / "academic_calendar" / f"{calendar['academic_year']}.json",
            calendar,
        )
        print(f"✔ ({len(calendar['events'])} events)")
        calendars.append(calendar)
    return calendars

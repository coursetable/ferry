import datetime
from pathlib import Path

from httpx import AsyncClient

from ferry.crawler.cache import save_cache_json

from .parse import ParsedAcademicCalendar, parse_academic_calendar

ACADEMIC_CALENDAR_URL = "https://catalog.yale.edu/ycps/calendar/"
ACADEMIC_CALENDAR_ARCHIVE_URL = (
    "https://catalog.yale.edu/archive/{academic_year}/ycps/calendar/"
)
# The catalog archive starts with the 2013-2014 edition
ARCHIVE_FIRST_YEAR = 2013


async def crawl_academic_calendar(
    data_dir: Path, client: AsyncClient
) -> ParsedAcademicCalendar:
    """
    Fetch the current Yale College academic calendar and save it to
    `academic_calendar/<academic_year>.json`. Previous years' files are kept, so
    the directory accumulates one file per academic year.
    """
    print("Fetching academic calendar...", end=" ")
    r = await client.get(ACADEMIC_CALENDAR_URL, follow_redirects=True)
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
    data_dir: Path, client: AsyncClient
) -> list[ParsedAcademicCalendar]:
    """
    Fetch every archived Yale College academic calendar from the catalog
    archive and save each to `academic_calendar/<academic_year>.json`.
    """
    calendars: list[ParsedAcademicCalendar] = []
    for year in range(ARCHIVE_FIRST_YEAR, datetime.date.today().year):
        url = ACADEMIC_CALENDAR_ARCHIVE_URL.format(academic_year=f"{year}-{year + 1}")
        print(f"Fetching {year}-{year + 1} academic calendar...", end=" ")
        r = await client.get(url, follow_redirects=True)
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

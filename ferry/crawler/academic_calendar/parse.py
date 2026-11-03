"""
Parses the Yale College Calendar with Pertinent Deadlines from the YCPS catalog.

The page for academic year Y-(Y+1) has one `<h2>` per term ("FALL Y", then
"SPRING Y+1"), each followed by a `table.tbl_calterm` with Date / Day /
Description columns. The headings are only used as a sanity check. Dates are
given without a year (e.g. "Sept. 15"), so we rely on the rows being in
chronological order: the first date is in year Y, and the year increments when
the month goes backwards. The listed day of the week is stored as-is but never
used (it doesn't always agree with the dates, which we trust instead).

Archived calendars before 2020-2021 use a different layout (a single
`table.caltable` with a `<th>` row per term and several `<br>`-separated events
per cell), so we don't bother parsing them.
"""

import datetime
import re
from collections.abc import Iterator
from typing import TypedDict

from bs4 import BeautifulSoup, Tag
from bs4.element import NavigableString


class ParsedCalendarEvent(TypedDict):
    season_code: str
    # ISO date strings (YYYY-MM-DD)
    date: str
    day_of_week: str
    description: str


class ParsedAcademicCalendar(TypedDict):
    academic_year: str
    source_url: str
    events: list[ParsedCalendarEvent]


class CalendarParseError(Exception):
    pass


MONTHS = {
    "jan": 1,
    "feb": 2,
    "mar": 3,
    "apr": 4,
    "may": 5,
    "jun": 6,
    "jul": 7,
    "aug": 8,
    "sep": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}

# Dates from this month on in the second year belong to the next academic year
NEXT_ACADEMIC_YEAR_START_MONTH = 7

# Matches "Sept. 15", "May 1", and ranges like "Mar. 6–22" or "Dec. 22–Jan. 11",
# for which we only keep the first day
date_re = re.compile(
    r"^(?P<month>[A-Za-z]+)\.?\s*(?P<day>\d{1,2})"
    + r"(?:\s*[–—-]\s*(?:[A-Za-z]+\.?\s*)?\d{1,2})?$"
)


def parse_month_day(date_text: str) -> tuple[int, int]:
    match = date_re.match(date_text.strip())
    if not match:
        raise CalendarParseError(f"Unrecognized date: {date_text!r}")
    key = match["month"].lower()[:3]
    if key not in MONTHS:
        raise CalendarParseError(f"Unknown month: {match['month']}")
    return MONTHS[key], int(match["day"])


def cell_lines(cell: Tag) -> list[str]:
    for br in cell.select("br"):
        br.replace_with(NavigableString("\n"))
    lines = cell.get_text().split("\n")
    return [" ".join(line.split()) for line in lines if line.strip()]


def iter_term_rows(soup: BeautifulSoup) -> Iterator[tuple[str, Tag]]:
    """Yield (term heading, row) pairs in page order."""
    for table in soup.select("table.tbl_calterm"):
        heading = table.find_previous("h2")
        if not isinstance(heading, Tag):
            raise CalendarParseError("Calendar table has no term heading")
        for row in table.select("tbody tr"):
            yield heading.get_text(" ", strip=True), row


def parse_academic_calendar(
    html: str, source_url: str, first_year: int
) -> ParsedAcademicCalendar | None:
    """
    Parse the calendar page for academic year `first_year`-`first_year + 1`.
    Returns None (with a warning) if the page's term headings don't match.
    """
    soup = BeautifulSoup(html, "lxml")
    academic_year = f"{first_year}-{first_year + 1}"

    expected_headings = [f"FALL {first_year}", f"SPRING {first_year + 1}"]
    headings = [
        " ".join(heading.split()).upper()
        for heading in dict.fromkeys(h for h, _ in iter_term_rows(soup))
    ]
    if headings != expected_headings:
        print(
            f"Warning: skipping {academic_year} calendar, expected term headings "
            + f"{expected_headings} but found {headings}"
        )
        return None

    events: list[ParsedCalendarEvent] = []
    year = first_year
    prev_month = 0
    for heading_text, row in iter_term_rows(soup):
        is_fall = heading_text.upper().startswith("FALL")
        season_code = f"{first_year}03" if is_fall else f"{first_year + 1}01"

        cells = row.select("td")
        if len(cells) != 3:
            continue
        date_text = " ".join(cells[0].get_text().split())
        day_text = " ".join(cells[1].get_text().split())
        descriptions = cell_lines(cells[2])
        if not date_text or not descriptions:
            continue

        month, day = parse_month_day(date_text)
        if month < prev_month:
            if year > first_year:
                print(
                    f"Warning: skipping out-of-order date {date_text!r} in "
                    + f"{academic_year} calendar: {descriptions}"
                )
                continue
            year += 1
        prev_month = month
        # Spring tables sometimes end with the next fall's dates (e.g. 2023-2024
        # lists "Aug. 28: Fall term classes begin"); those belong to next year
        if year > first_year and month >= NEXT_ACADEMIC_YEAR_START_MONTH:
            print(
                f"Warning: skipping next academic year's date {date_text!r} in "
                + f"{academic_year} calendar: {descriptions}"
            )
            continue

        date = datetime.date(year, month, day)
        for description in descriptions:
            events.append(
                {
                    "season_code": season_code,
                    "date": date.isoformat(),
                    "day_of_week": day_text,
                    "description": description,
                }
            )

    if not events:
        print(f"Warning: skipping {academic_year} calendar, no events found")
        return None

    return {
        "academic_year": academic_year,
        "source_url": source_url,
        "events": events,
    }

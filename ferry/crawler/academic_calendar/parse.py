"""
Parses the Yale College Calendar with Pertinent Deadlines from the YCPS catalog.

Since 2020-2021, the page has one `<h2>` per term (e.g. "FALL 2026"), each
followed by a `table.tbl_calterm` with Date / Day / Description columns. Before
that, archived pages use a single `table.caltable` with a `<th>` row per term
(e.g. "Fall Term 2019") and several `<br>`-separated events per cell. Dates are
given without a year (e.g. "Sept. 15"), so we infer the year from the term
heading and sanity-check it against the listed day of the week.
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


TERM_TO_SEASON_SUFFIX = {"spring": "01", "summer": "02", "fall": "03"}

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

# Day abbreviations used on the page, mapped to datetime.weekday()
WEEKDAYS = {"M": 0, "T": 1, "W": 2, "Th": 3, "F": 4, "S": 5, "Su": 6}

# The pages occasionally have typos in the day of week (e.g. 2021-2022 lists
# Tuesday May 3 as "M"), so only fail if mismatches look like a year bug
MAX_DAY_MISMATCH_RATIO = 0.05

term_heading_re = re.compile(
    r"^(fall|spring|summer)(?:\s+(?:term|session))?\s+(\d{4})$", re.I
)
academic_year_re = re.compile(r"(\d{4})\s*[–—-]\s*(\d{4})")
# Matches "Sept. 15", "May 1", and ranges like "Mar. 6–22" or "Dec. 22–Jan. 11",
# for which we only keep the first day
date_re = re.compile(
    r"^(?P<month>[A-Za-z]+)\.?\s*(?P<day>\d{1,2})"
    + r"(?:\s*[–—-]\s*(?:[A-Za-z]+\.?\s*)?\d{1,2})?$"
)


def parse_month(month: str) -> int:
    key = month.lower()[:3]
    if key not in MONTHS:
        raise CalendarParseError(f"Unknown month: {month}")
    return MONTHS[key]


def infer_year(term: str, term_year: int, month: int) -> int:
    # Fall term calendars can spill into January (e.g. final grades due), and
    # spring term calendars can start in the preceding fall (e.g. registration)
    if term == "fall" and month < 7:
        return term_year + 1
    if term == "spring" and month > 7:
        return term_year - 1
    return term_year


def parse_date(date_text: str, term: str, term_year: int) -> datetime.date:
    match = date_re.match(date_text.strip())
    if not match:
        raise CalendarParseError(f"Unrecognized date: {date_text!r}")

    month = parse_month(match["month"])
    return datetime.date(infer_year(term, term_year, month), month, int(match["day"]))


def normalize_day(day_text: str) -> str:
    # Older pages use "TH" instead of "Th"
    return day_text.capitalize() if day_text.capitalize() in WEEKDAYS else day_text


def day_matches(day_text: str, date: datetime.date) -> bool:
    # Only validate single days; ranges are listed like "F–Su"
    return day_text not in WEEKDAYS or WEEKDAYS[day_text] == date.weekday()


def cell_lines(cell: Tag) -> list[str]:
    for br in cell.select("br"):
        br.replace_with(NavigableString("\n"))
    lines = cell.get_text().split("\n")
    return [" ".join(line.split()) for line in lines if line.strip()]


def iter_term_rows(soup: BeautifulSoup) -> Iterator[tuple[str, Tag]]:
    """Yield (term heading, row) pairs for both page layouts."""
    for table in soup.select("table.tbl_calterm"):
        heading = table.find_previous("h2")
        if not isinstance(heading, Tag):
            raise CalendarParseError("Calendar table has no term heading")
        for row in table.select("tbody tr"):
            yield heading.get_text(" ", strip=True), row

    for table in soup.select("table.caltable"):
        heading_text = None
        for row in table.select("tr"):
            if th := row.select_one("th"):
                heading_text = th.get_text(" ", strip=True)
            elif heading_text is None:
                raise CalendarParseError("Calendar row has no term heading")
            else:
                yield heading_text, row


def parse_academic_year(soup: BeautifulSoup, term_years: dict[str, int]) -> str:
    # The catalog title looks like "Yale College Programs of Study 2026–2027"
    for h1 in soup.find_all("h1"):
        if match := academic_year_re.search(h1.get_text()):
            return f"{match[1]}-{match[2]}"
    if "fall" in term_years:
        return f"{term_years['fall']}-{term_years['fall'] + 1}"
    if "spring" in term_years:
        return f"{term_years['spring'] - 1}-{term_years['spring']}"
    raise CalendarParseError("Could not determine academic year")


def parse_academic_calendar(html: str, source_url: str) -> ParsedAcademicCalendar:
    soup = BeautifulSoup(html, "lxml")

    events: list[ParsedCalendarEvent] = []
    term_years: dict[str, int] = {}
    day_mismatches: list[str] = []
    for heading_text, row in iter_term_rows(soup):
        heading_match = term_heading_re.match(heading_text)
        if not heading_match:
            raise CalendarParseError(f"Unrecognized term heading: {heading_text!r}")

        term = heading_match[1].lower()
        term_year = int(heading_match[2])
        term_years[term] = term_year
        season_code = f"{term_year}{TERM_TO_SEASON_SUFFIX[term]}"

        cells = row.select("td")
        if len(cells) != 3:
            continue
        date_text = " ".join(cells[0].get_text().split())
        day_text = normalize_day(" ".join(cells[1].get_text().split()))
        descriptions = cell_lines(cells[2])
        if not date_text or not descriptions:
            continue

        date = parse_date(date_text, term, term_year)
        if not day_matches(day_text, date):
            day_mismatches.append(
                f"{date_text!r}: page says {day_text}, "
                + f"but inferred date {date.isoformat()} is a {date.strftime('%A')}"
            )
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
        raise CalendarParseError("No calendar events found")
    if len(day_mismatches) > MAX_DAY_MISMATCH_RATIO * len(events):
        raise CalendarParseError(
            "Too many day of week mismatches:\n" + "\n".join(day_mismatches)
        )
    for mismatch in day_mismatches:
        print(f"Warning: day of week mismatch for {mismatch}")

    return {
        "academic_year": parse_academic_year(soup, term_years),
        "source_url": source_url,
        "events": events,
    }

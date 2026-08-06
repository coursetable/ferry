import re
from typing import TypedDict, cast

from bs4 import BeautifulSoup, ResultSet, Tag

STAT_TYPES = ["REGISTERED", "WAITLISTED", "VISITING"]

class EmptyDemandError(Exception):
    pass


def normalize_label(text: str) -> str:
    return re.sub(r"\s+", " ", text.replace("\xa0", " ")).strip()


def parse_int(text: str) -> int:
    try:
        return int(text.strip())
    except ValueError:
        return 0


def parse_stat_table(
    soup: BeautifulSoup, stat_type: str
) -> dict[tuple[str, str], int] | None:
    """
    Parses a single course-detail-{STAT_TYPE} table into a
    {(listing, date): count} mapping. Drops the redundant Total column.
    """
    table = soup.find("table", id=f"course-detail-{stat_type}")
    if type(table) != Tag:
        return None

    thead = table.find("thead")
    tbody = table.find("tbody")
    if type(thead) != Tag or type(tbody) != Tag:
        return None

    header_cells = cast(ResultSet[Tag], thead.find("tr").find_all("th"))
    # first column is Date, last column is Total
    listing_labels = [normalize_label(h.get_text()) for h in header_cells[1:-1]]

    counts: dict[tuple[str, str], int] = {}
    for row in cast(ResultSet[Tag], tbody.find_all("tr")):
        cells = cast(ResultSet[Tag], row.find_all("td"))
        date = cells[0].get_text().strip()
        for listing, cell in zip(listing_labels, cells[1:-1]):
            counts[(listing, date)] = parse_int(cell.get_text())

    return counts


class DemandCount(TypedDict):
    listing: str
    date: str
    registered: int
    waitlisted: int
    visiting: int


class ParsedCourseDemand(TypedDict):
    season_code: str
    subject_code: str
    course_number: str
    course_title: str
    counts: list[DemandCount]


def parse_course_demand_page(
    page: bytes | None,
    season_code: str,
    subject_code: str,
    course_number: str,
) -> ParsedCourseDemand | None:
    if page is None:
        return None

    soup = BeautifulSoup(page, "lxml")

    title_elem = soup.find("h3", id="courseTitles")
    if title_elem is None:
        # Either an auth/error page, or this course has no demand data.
        return None
    course_title = normalize_label(title_elem.get_text())

    stat_tables = {
        stat_type: parse_stat_table(soup, stat_type) for stat_type in STAT_TYPES
    }
    if all(table is None for table in stat_tables.values()):
        raise EmptyDemandError(
            f"No demand data found for {season_code}-{subject_code}{course_number}"
        )

    all_keys: set[tuple[str, str]] = set()
    for table in stat_tables.values():
        if table is not None:
            all_keys.update(table.keys())

    counts: list[DemandCount] = [
        {
            "listing": listing,
            "date": date,
            "registered": (stat_tables["REGISTERED"] or {}).get((listing, date), 0),
            "waitlisted": (stat_tables["WAITLISTED"] or {}).get((listing, date), 0),
            "visiting": (stat_tables["VISITING"] or {}).get((listing, date), 0),
        }
        for listing, date in sorted(all_keys)
    ]

    return {
        "season_code": season_code,
        "subject_code": subject_code,
        "course_number": course_number,
        "course_title": course_title,
        "counts": counts,
    }

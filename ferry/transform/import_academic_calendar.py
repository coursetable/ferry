from pathlib import Path

import pandas as pd
import ujson

from ferry.crawler.academic_calendar.parse import ParsedAcademicCalendar

ACADEMIC_CALENDAR_COLUMNS = [
    "academic_year",
    "season_code",
    "date",
    "end_date",
    "day_of_week",
    "description",
]


def import_academic_calendar(data_dir: Path) -> pd.DataFrame:
    """
    Build the `academic_calendar_events` table from every parsed academic
    calendar in `academic_calendar/`.
    """
    rows = []
    for path in sorted((data_dir / "academic_calendar").glob("*.json")):
        with open(path) as f:
            calendar: ParsedAcademicCalendar = ujson.load(f)
        for event in calendar["events"]:
            rows.append({"academic_year": calendar["academic_year"], **event})

    events = pd.DataFrame(rows, columns=pd.Index(ACADEMIC_CALENDAR_COLUMNS))
    events = events.drop_duplicates(subset=["season_code", "date", "description"])
    events = events.sort_values(["date", "season_code"], kind="stable")
    events = events.reset_index(drop=True)
    events.insert(0, "id", events.index + 1)

    print(f"Total academic calendar events: {len(events)}")
    return events

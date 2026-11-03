import re
from pathlib import Path

import pandas as pd
import ujson

from ferry.crawler.academic_calendar.parse import ParsedAcademicCalendar

ACADEMIC_CALENDAR_COLUMNS = [
    "academic_year",
    "season_code",
    "date",
    "day_of_week",
    "description",
    "event_type",
]

# Yale words the same milestone differently across years (e.g. "Fall-term
# classes begin, 8:20 a.m." vs "Fall classes begin."), so we tag milestones with
# a stable event_type for querying. Patterns are matched against the start of
# each ";"-separated clause of a description.
EVENT_TYPE_PATTERNS = {
    "classes_begin": re.compile(r"^(fall|spring)(-| )(term )?classes begin"),
    "classes_end": re.compile(
        r"^(yale college |(fall|spring)-term )?(online )?classes end"
    ),
    "reading_period_begins": re.compile(r"^(online )?reading period begins"),
    "reading_period_ends": re.compile(r"^(online )?reading period ends"),
    "final_exams_begin": re.compile(r"^(online )?final examinations begin"),
    "final_exams_end": re.compile(r"^(online )?(final )?examination(s| period) end"),
    "mlk_day": re.compile(r"^martin luther king"),
}


def classify_clause(clause: str) -> str | None:
    clause = clause.strip().lower()
    for event_type, pattern in EVENT_TYPE_PATTERNS.items():
        if pattern.match(clause):
            return event_type
    return None


def split_and_classify(description: str) -> list[tuple[str, str | None]]:
    """
    Tag a description with an event type. Descriptions that combine several
    milestones (e.g. "Classes end; reading period begins.") are split into one
    event per milestone, so that each event type is a separate row.
    """
    clauses = description.split(";")
    types = [classify_clause(clause) for clause in clauses]
    if sum(t is not None for t in types) <= 1:
        return [(description, next((t for t in types if t), None))]

    events: list[tuple[list[str], str | None]] = []
    for clause, event_type in zip(clauses, types, strict=True):
        if event_type is not None or not events:
            events.append(([clause.strip()], event_type))
        else:
            events[-1][0].append(clause.strip())
    return [
        (text[0].upper() + text[1:], event_type)
        for text, event_type in ((";".join(parts), t) for parts, t in events)
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
            for description, event_type in split_and_classify(event["description"]):
                # Only tagged milestones go to the database
                if event_type is None:
                    continue
                rows.append(
                    {
                        **event,
                        "academic_year": calendar["academic_year"],
                        "description": description,
                        "event_type": event_type,
                    }
                )

    events = pd.DataFrame(rows, columns=pd.Index(ACADEMIC_CALENDAR_COLUMNS))
    events = events.drop_duplicates(subset=["season_code", "date", "description"])
    events = events.sort_values(["date", "season_code"], kind="stable")
    events = events.reset_index(drop=True)
    events.insert(0, "id", events.index + 1)

    # Queries rely on there being at most one event per type per season
    duplicates = events.duplicated(subset=["season_code", "event_type"], keep=False)
    for _, event in events.loc[duplicates].iterrows():
        print(
            f"Warning: multiple {event['event_type']} events in "
            + f"{event['season_code']}: {event['date']} {event['description']!r}"
        )

    print(f"Total academic calendar events: {len(events)}")
    return events

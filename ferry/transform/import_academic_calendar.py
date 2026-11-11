import datetime
import re
from pathlib import Path

import pandas as pd
import ujson

from ferry.crawler.academic_calendar.parse import (
    ParsedAcademicCalendar,
    ParsedCalendarEvent,
)

COLUMNS = [
    "season_code",
    "event_type",
    "start_date",
    "end_date",
    "transfer_day",
]

# Event types that occur at most once per season
EVENT_TYPES = {
    "classes",
    "reading_period",
    "final_exams",
    "labor_day",
    "mlk_day",
    "october_recess",
    "november_recess",
    "spring_recess",
}

# Matches "Monday classes meet instead." as 1 (Monday) through 5 (Friday)
TRANSFER_DAYS = {"monday": 1, "tuesday": 2, "wednesday": 3, "thursday": 4, "friday": 5}

# Yale words the same milestone differently across years (e.g. "Fall-term
# classes begin, 8:20 a.m." vs "Fall classes begin."), and a single description
# can contain several (e.g. "Classes end; reading period begins."), so we search
# each description for every marker, in the order they appear.
# Each marker is (event type, role, pattern). Roles:
# - start / end: the first / last day of a range, paired with the next end
# - recess_start: recess begins after the last class that day, so the range
#   starts the next day; paired with the next "Classes resume"
# - recess_end: classes resume that day, so the recess ends the day before
# - single: a one-day event
# - transfer: that day, classes for another weekday meet instead
MARKERS: list[tuple[str, str, re.Pattern[str]]] = [
    (
        "classes",
        "start",
        re.compile(r"\b(?:fall|spring)(?:-term)? (?:online )?classes begin", re.I),
    ),
    # Excludes "Graduate School classes end" and "recess begins after classes end"
    ("classes", "end", re.compile(r"(?<!school )(?<!after )\bclasses end", re.I)),
    ("reading_period", "start", re.compile(r"reading period begins", re.I)),
    ("reading_period", "end", re.compile(r"reading period ends", re.I)),
    ("final_exams", "start", re.compile(r"final examinations begin", re.I)),
    (
        "final_exams",
        "end",
        re.compile(r"examinations end|examination period ends", re.I),
    ),
    (
        "{}_recess",
        "recess_start",
        re.compile(r"\b(october|november|spring) recess begins", re.I),
    ),
    ("recess", "recess_end", re.compile(r"classes resume", re.I)),
    # 2020-2021 lists "Labor Day; classes meet."
    ("labor_day", "single", re.compile(r"labor day(?!; classes meet\b)", re.I)),
    ("mlk_day", "single", re.compile(r"martin luther king", re.I)),
    # 2020-2021 spring has standalone break days instead of a spring recess
    ("no_classes", "single", re.compile(r"^classes do not meet", re.I)),
    (
        "transfer",
        "transfer",
        re.compile(
            r"\b(monday|tuesday|wednesday|thursday|friday) classes meet instead", re.I
        ),
    ),
]


def find_markers(description: str) -> list[tuple[str, str, str | None]]:
    """Return (event type, role, captured weekday or term) in text order."""
    found: list[tuple[int, str, str, str | None]] = []
    for event_type, role, pattern in MARKERS:
        for match in pattern.finditer(description):
            group = match[1].lower() if pattern.groups else None
            name = event_type

            if role == "recess_start":
                assert group is not None
                name = event_type.format(group)

            found.append((match.start(), name, role, group))
    return [(event_type, role, group) for _, event_type, role, group in sorted(found)]


def build_season_ranges(
    season_code: str, events: list[ParsedCalendarEvent]
) -> list[dict[str, str | int | None]]:
    """
    Pair up a season's start and end markers chronologically: each start is
    closed by the next matching end (e.g. "October recess begins" by the next
    "Classes resume").
    """
    rows: list[dict[str, str | int | None]] = []
    open_starts: dict[str, datetime.date] = {}
    open_recess: tuple[str, datetime.date] | None = None

    def add(
        event_type: str,
        start: datetime.date,
        end: datetime.date,
        transfer_day: int | None = None,
    ):
        rows.append(
            {
                "season_code": season_code,
                "event_type": event_type,
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "transfer_day": transfer_day,
            }
        )

    def warn(message: str):
        print(f"Warning: {season_code}: {message}")

    for event in events:
        date = datetime.date.fromisoformat(event["date"])
        for event_type, role, group in find_markers(event["description"]):
            if role == "start":
                if event_type in open_starts:
                    warn(f"{event_type} starts again on {date} before ending")
                open_starts[event_type] = date

            elif role == "end":
                if event_type not in open_starts:
                    warn(f"{event_type} ends on {date} without starting")
                    continue
                add(event_type, open_starts.pop(event_type), date)

            elif role == "recess_start":
                if open_recess is not None:
                    warn(f"{event_type} begins on {date} during {open_recess[0]}")
                open_recess = (event_type, date + datetime.timedelta(days=1))

            elif role == "recess_end":
                if open_recess is None:
                    warn(f"classes resume on {date} without a recess")
                    continue

                add(open_recess[0], open_recess[1], date - datetime.timedelta(days=1))
                open_recess = None

            elif role == "single":
                add(event_type, date, date)

            elif role == "transfer":
                assert group is not None
                add(event_type, date, date, TRANSFER_DAYS[group])

    for event_type, start in open_starts.items():
        warn(f"{event_type} starts on {start} but never ends")

    if open_recess is not None:
        warn(f"{open_recess[0]} starts on {open_recess[1]} but classes never resume")
    return rows


def import_academic_calendar(data_dir: Path) -> pd.DataFrame:
    """
    Build the `academic_calendar_events` table from every parsed academic
    calendar in `academic_calendar/`. Events without a recognized marker are
    not stored.
    """
    rows: list[dict[str, str | int | None]] = []

    for path in sorted((data_dir / "academic_calendar").glob("*.json")):
        with open(path) as f:
            calendar: ParsedAcademicCalendar = ujson.load(f)
        seasons: dict[str, list[ParsedCalendarEvent]] = {}

        for event in calendar["events"]:
            seasons.setdefault(event["season_code"], []).append(event)

        for season_code, events in seasons.items():
            rows.extend(build_season_ranges(season_code, events))

    events = pd.DataFrame(rows, columns=pd.Index(COLUMNS))
    events["transfer_day"] = events["transfer_day"].astype("Int64")
    events = events.sort_values(["start_date", "season_code"], kind="stable")
    events = events.reset_index(drop=True)
    events.insert(0, "id", events.index + 1)

    unique = events.loc[events["event_type"].isin(list(EVENT_TYPES))]
    duplicates = unique.duplicated(subset=["season_code", "event_type"], keep=False)

    for _, event in unique.loc[duplicates].iterrows():
        print(
            f"Warning: multiple {event['event_type']} events in "
            + f"{event['season_code']}: {event['start_date']}"
        )

    print(f"Total academic calendar events: {len(events)}")
    return events

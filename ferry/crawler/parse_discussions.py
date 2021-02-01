"""
Loads the class JSON files output by fetch_classes.py and
formats them for input into transform.py
"""

import argparse
from os import listdir
from typing import Dict, Tuple

import pandas as pd
import tabula

from ferry import config
from ferry.crawler.common_args import add_seasons_args


# allow the user to specify seasons
parser = argparse.ArgumentParser(description="Parse discussion sections")
add_seasons_args(parser)

args = parser.parse_args()
seasons = args.seasons

# folder to load discussion sections from
raw_discussions_folder = config.DATA_DIR / "discussion_sections" / "raw_pdfs"
# folder to save discussion sections to
parsed_discussions_folder = config.DATA_DIR / "discussion_sections" / "parsed_csvs"

if seasons is None:

    # get seasons from fetched raw JSON file names
    seasons = [
        filename.split(".")[0]
        for filename in listdir(raw_discussions_folder)
        if filename.endswith(".pdf")
    ]

    seasons = sorted(seasons)

print(f"Parsing discussion sections for season(s): {seasons}")

DAYS_MAP = {
    "M": "Monday",
    "T": "Tuesday",
    "W": "Wednesday",
    "H": "Thursday",
    "F": "Friday",
    "Sa": "Saturday",
    "Su": "Sunday",
}


def parse_location_times(
    raw_time: str,
) -> Tuple[str, str, str, Dict[str, Tuple[str, str, str]]]:
    """
    Parse out meeting times and locations for database.

    Parameters
    ----------
    raw_time:
        The raw time (and location if specified) extracted from the rightmost column of the
        discussion sections PDF.

    Returns
    -------
    times_summary:
        Summary of meeting times.
    locations_summary:
        Summary of meeting location(s).
    times_long_summary:
        Summary of meeting times and locations. Currently '{times_summary} in {locations_summary}'
        if location is specified, and equivalent to times_summary if location not specified.
    times_by_day:
        Dictionary with keys as days and values consisting of lists of
        [start_time, end_time, location].
    """
    # return empty values if raw time string is itself empty
    if raw_time == "":
        return "", "", "", {}

    time_split = raw_time.split(" ", maxsplit=2)

    days_raw = time_split[0]
    time = time_split[1]

    # replace Thursday so single-letter matching works
    # otherwise, Tuesday (T) also gets matched
    days = [
        day_full
        for day_short, day_full in DAYS_MAP.items()
        if day_short in days_raw.replace("Th", "H")
    ]

    # location isn't always provided (especially with online courses, so set an empty default)
    location = ""

    if len(time_split) == 3:
        location = time_split[2]

    start_time, end_time = time.split("-")

    # ambiguous evening times have a 'p' appended
    end_p = end_time[-1] == "p"
    if end_p:
        end_time = end_time[:-1]

    start_hour_, start_minute_ = start_time.split(".")
    end_hour_, end_minute_ = end_time.split(".")

    start_hour = int(start_hour_)
    start_minute = int(start_minute_)
    end_hour = int(end_hour_)
    end_minute = int(end_minute_)

    start_pm = False
    end_pm = False

    # if start hour is probably in the afternoon
    if 1 <= start_hour <= 6:
        start_pm = True

    # if end hour is probably in the afternoon
    if 1 <= end_hour <= 6:

        end_pm = True

    # handle cases where the start hour is
    # 7, 8, 9... pm
    if end_p:

        start_pm = True
        end_pm = True

    if start_pm:
        start_hour += 12
    if end_pm:
        end_hour += 12

    # quick map for AM/PM for formatting
    am_pm = {False: "am", True: "pm"}

    # AM/PM formatted start and end times
    start_time_formatted = f"{start_hour_}:{start_minute:02d}{am_pm[start_pm]}"
    end_time_formatted = f"{end_hour_}:{end_minute:02d}{am_pm[end_pm]}"

    # 24-hour formatted start and end times
    start_time_24_formatted = f"{start_hour}:{start_minute:02d}"
    end_time_24_formatted = f"{end_hour}:{end_minute:02d}"

    times_summary = f"{days_raw} {start_time_formatted}-{end_time_formatted}"
    locations_summary = location
    times_long_summary = f"{times_summary}"
    if location != "":
        times_long_summary = f"{times_summary} in {location}"
    times_by_day = {
        day: (
            start_time_24_formatted,
            end_time_24_formatted,
            location,
        )
        for day in days
    }

    return times_summary, locations_summary, times_long_summary, times_by_day


def parse_discussions(season: str):
    discussions = tabula.read_pdf(
        config.DATA_DIR / "discussion_sections" / "raw_pdfs" / f"{season}.pdf",
        pandas_options={
            "names": ["section_crn", "subject", "number", "section", "info", "time"],
            "dtype": {"section": str},
        },
        multiple_tables=False,
        stream=True,
        pages="all",
    )[0]

    discussions["subject"].fillna(method="ffill", inplace=True)
    discussions["number"].fillna(method="ffill", inplace=True)
    discussions["section"].fillna(method="ffill", inplace=True)
    discussions["info"].fillna(method="ffill", inplace=True)
    discussions["time"].fillna(value="", inplace=True)

    def patch_code(row):
        """
        Fix discussion section code parse errors.

        Sometimes tabula parses the subject and code into the same column.
        """

        if " " in row["subject"]:
            row["subject"], row["number"] = row["subject"].split(" ", maxsplit=1)

        return row

    discussions = discussions.apply(patch_code, axis=1)

    discussions["section_crn"] = discussions["section_crn"].astype("Int64")

    discussions["time"] = discussions["time"].fillna("")

    (
        discussions["times_summary"],
        discussions["locations_summary"],
        discussions["times_long_summary"],
        discussions["times_by_day"],
    ) = zip(*discussions["time"].apply(parse_location_times))

    discussions.to_csv(
        config.DATA_DIR / "discussion_sections" / "parsed_csvs" / f"{season}.csv",
        index=False,
    )


# load list of classes per season
for season in seasons:

    print(f"Parsing discussion sections for season {season}")

    parse_discussions(season)

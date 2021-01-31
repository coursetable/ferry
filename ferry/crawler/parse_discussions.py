"""
Loads the class JSON files output by fetch_classes.py and
formats them for input into transform.py
"""

import argparse
from os import listdir

import pandas as pd

from ferry import config
from ferry.crawler.common_args import add_seasons_args

# allow the user to specify seasons
parser = argparse.ArgumentParser(description="Parse discussion sections")
add_seasons_args(parser)

args = parser.parse_args()
seasons = args.seasons

# folder to load discussion sections from
raw_discussions_folder = config.DATA_DIR / "discussion_sections" / "raw_csvs"
# folder to save discussion sections to
parsed_discussions_folder = config.DATA_DIR / "discussion_sections" / "parsed_csvs"

if seasons is None:

    # get seasons from fetched raw JSON file names
    seasons = [
        filename.split(".")[0]
        for filename in listdir(raw_discussions_folder)
        if filename.endswith(".csv")
    ]

    seasons = sorted(seasons)

print(f"Parsing discussion sections for season(s): {seasons}")

DAYS_MAP = {
    "M": "Monday",
    "T": "Tuesday",
    "W": "Wednesday",
    "Th": "Thursday",
    "F": "Friday",
    "Sa": "Saturday",
    "Su": "Sunday",
}

def parse_location_times(raw_time):
    if raw_time == "":
        return ("","","","")
    
    time_split = raw_time.split(" ",maxsplit=2)

    day_ = time_split[0]
    time = time_split[1]

    day = DAYS_MAP[day]

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
        end_hour+=12

    am_pm = {
        False:"am",
        True: "pm"
    }

    start_time_formatted = f"{start_hour_}:{start_minute:02d}{am_pm[start_pm]}"
    end_time_formatted = f"{end_hour_}:{end_minute:02d}{am_pm[end_pm]}"

    start_time_24_formatted = f"{start_hour}:{start_minute:02d}"
    end_time_24_formatted = f"{end_hour}:{end_minute:02d}"

    times_summary = f"{day} {start_time_formatted}-{end_time_formatted}"
    locations_summary = location
    times_long_summary = f"{times_summary} in {locations_summary}"
    times_by_day = {
        day: [
            start_time_24_formatted,
            end_time_24_formatted,
            location,
        ]
    }

    # return start_hour, end_hour, time
    # return day, start_time, end_time, start_hour, end_hour, location, time
    # return day

# load list of classes per season
for season in seasons:

    print(f"Parsing discussion sections for season {season}")

    # load raw responses for season
    season_discussions = pd.read_csv(config.DATA_DIR / "discussion_sections" / "raw_csvs" / f"{season}.csv")

    season_discussions["time"] = season_discussions["time"].fillna("")

    print(season_discussions)

    # # write output
    # with open(parsed_courses_folder / "{season}.csv", "w") as f:
    #     ujson.dump(parsed_course_info, f, indent=4)

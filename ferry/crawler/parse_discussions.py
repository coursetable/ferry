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
        if filename.endswith(".json")
    ]

    seasons = sorted(seasons)

print(f"Parsing discussion sections for season(s): {seasons}")

# load list of classes per season
for season in seasons:

    print(f"Parsing discussion sections for season {season}")

    # load raw responses for season
    season_discussions = pd.read_csv(config.DATA_DIR / "discussion_sections" / "raw_csvs" / f"{season}.csv")

    print(season_discussions)

    # # write output
    # with open(parsed_courses_folder / "{season}.csv", "w") as f:
    #     ujson.dump(parsed_course_info, f, indent=4)

"""
Loads the class JSON files output by fetch_classes.py and
formats them for input into transform.py
"""

import argparse
from os import listdir
from pathlib import Path

import ujson

from ferry import config
from ferry.crawler.common_args import add_seasons_args
from ferry.includes.class_parsing import extract_course_info
from ferry.includes.tqdm import tqdm

# allow the user to specify seasons
parser = argparse.ArgumentParser(description="Parse classes")
add_seasons_args(parser)

args = parser.parse_args()
seasons = args.seasons

# folder to save course infos to
parsed_courses_folder = f"{config.DATA_DIR}/parsed_courses/"

if seasons is None:

    # get seasons from fetched raw JSON file names
    seasons = [
        filename.split(".")[0]
        for filename in listdir(f"{config.DATA_DIR}/course_json_cache/")
        if filename.endswith(".json")
    ]

    seasons = sorted(seasons)

print(f"Parsing courses for season(s): {seasons}")

# load list of classes per season
for season in seasons:

    print(f"Parsing courses for season {season}")

    fysem_file = Path(f"{config.DATA_DIR}/season_courses/{season}_fysem.json")

    if fysem_file.is_file():
        with open(fysem_file, "r") as f:
            fysem = ujson.load(f)
            fysem = {x["crn"] for x in fysem}
        print("Loaded first-year seminars")
    else:
        print("First-year seminars filter missing")
        fysem = set()

    # load raw responses for season
    with open(f"{config.DATA_DIR}/course_json_cache/{season}.json", "r") as file:
        aggregate_term_json = ujson.load(file)

    # parse course JSON in season
    parsed_course_info = [
        extract_course_info(x, season, fysem)
        for x in tqdm(aggregate_term_json, ncols=96)
    ]

    # write output
    with open(f"{config.DATA_DIR}/parsed_courses/{season}.json", "w") as file:
        ujson.dump(parsed_course_info, file, indent=4)

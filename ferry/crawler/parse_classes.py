import argparse
from os import listdir
from os.path import isfile, join

import ujson
from tqdm import tqdm

from ferry import config
from ferry.includes.class_processing import *

"""
================================================================
This script loads the class JSON files output by
fetch_classes.py and formats them to be loadable into the
current website.
================================================================
"""

# allow the user to specify seasons (useful for testing and debugging)
parser = argparse.ArgumentParser(description="Parse classes")
parser.add_argument(
    "-s",
    "--seasons",
    nargs="+",
    help="seasons to parse (leave empty to parse all)",
    default=None,
    required=False,
)

args = parser.parse_args()
seasons = args.seasons

# folder to save course infos to
parsed_courses_folder = f"{config.DATA_DIR}/parsed_courses/"

if seasons is None:

    # get the list of all course JSON files as previously fetched
    with open(f"{config.DATA_DIR}/seasons.json", "r") as f:
        seasons = ujson.load(f)

# load list of classes per season
for season in seasons:

    print(f"Parsing courses for season {season}")

    # load raw responses for season
    with open(f"{config.DATA_DIR}/course_json_cache/{season}.json", "r") as f:
        aggregate_term_json = ujson.load(f)

    # parse course JSON in season
    parsed_course_info = [extract_course_info(x, season) for x in aggregate_term_json]

    # write output
    with open(f"{config.DATA_DIR}/parsed_courses/{season}.json", "w") as f:
        f.write(ujson.dumps(parsed_course_info, indent=4))
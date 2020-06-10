from tqdm import tqdm
import json

import sys
sys.path.append("..")

from includes.class_processing import *

import argparse

"""
================================================================
This script fetches the following information from the Yale 
Courses API, in JSON format:

    (1) A list of all seasons with course info
        (/api_output/seasons.json)

    (2) A list of all courses for each season
        (/api_output/season_courses/)

    (3) Detailed information for each course, for each season
        (/api_output/course_json_cache/)
================================================================
"""

# -----------------------------------------
# Retrieve courses from unofficial Yale API
# -----------------------------------------

# allow the user to specify seasons (useful for testing and debugging)
parser = argparse.ArgumentParser(description='Fetch classes')
parser.add_argument('-s',
                    '--seasons',
                    nargs='+',
                    help='seasons to fetch (leave empty to fetch all)',
                    default=None,
                    required=False)

args = parser.parse_args()
seasons = args.seasons

if seasons is None:

    # list of all available seasons
    seasons = fetch_seasons()

    with open("../api_output/seasons.json", "w") as f:
        f.write(json.dumps(seasons, indent=4))

# get lists of classes per season
for season in seasons:
    print(f"Fetching class list for season {season}")

    season_courses = fetch_season_courses(season)

    # cache list of classes
    with open(f"../api_output/season_courses/{season}.json", "w") as f:
        f.write(json.dumps(season_courses, indent=4))

# fetch detailed info for each class in each season
for season in seasons:

    with open(f"../api_output/season_courses/{season}.json", "r") as f:
        season_courses = json.load(f)

    # track progress for each season
    pbar = tqdm(total=len(season_courses), ncols=96)
    pbar.set_description(f"Fetching class information for season {season}")

    # merge all the JSON results per season
    aggregate_season_json = []

    for course in season_courses:

        course_json = fetch_course_json(
            course["code"],
            course["crn"],
            course["srcdb"]
        )

        aggregate_season_json.append(course_json)

        pbar.update(1)

    # cache to JSON for entire season
    with open(f"../api_output/course_json_cache/{season}.json", "w") as f:
        f.write(json.dumps(aggregate_season_json, indent=4))

    print()
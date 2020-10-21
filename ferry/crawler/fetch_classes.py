import argparse

import ujson

from ferry import config
from ferry.includes.class_processing import (
    FetchClassesError,
    fetch_course_json,
    fetch_season_courses,
)
from ferry.includes.tqdm import tqdm

"""
================================================================
This script fetches the following information from the Yale 
Courses API, in JSON format:

    (1) A list of all courses for each season
        (/api_output/season_courses/)

    (2) Detailed information for each course, for each season
        (/api_output/course_json_cache/)
================================================================
"""

# -----------------------------------------
# Retrieve courses from unofficial Yale API
# -----------------------------------------

# allow the user to specify seasons (useful for testing and debugging)
parser = argparse.ArgumentParser(description="Fetch classes")
parser.add_argument(
    "-s",
    "--seasons",
    nargs="+",
    help="seasons to fetch (leave empty to fetch all, or LATEST_[n] to fetch n latest)",
    default=None,
    required=False,
)

args = parser.parse_args()

# list of seasons previously from fetch_seasons.py
with open(f"{config.DATA_DIR}/course_seasons.json", "r") as f:
    all_viable_seasons = ujson.loads(f.read())

# if no seasons supplied, use all
if args.seasons is None:

    seasons = all_viable_seasons

    print(f"Fetching courses for all seasons: {seasons}")

else:

    seasons_latest = len(args.seasons) == 1 and args.seasons[0].startswith("LATEST")

    # if fetching latest n seasons, truncate the list and log it
    if seasons_latest:

        num_latest = int(args.seasons[0].split("_")[1])

        seasons = all_viable_seasons[-num_latest:]

        print(f"Fetching courses for latest {num_latest} seasons: {seasons}")

    # otherwise, use and check the user-supplied seasons
    else:

        # Check to make sure user-inputted seasons are valid
        if all(season in all_viable_seasons for season in args.seasons):

            seasons = args.seasons
            print(f"Fetching courses for supplied seasons: {seasons}")

        else:
            raise FetchClassesError("Invalid season.")

# get lists of classes per season
for season in seasons:
    print(f"Fetching class list for season {season}")

    season_courses = fetch_season_courses(season)

    # cache list of classes
    with open(f"{config.DATA_DIR}/season_courses/{season}.json", "w") as f:
        f.write(ujson.dumps(season_courses, indent=4))

# fetch detailed info for each class in each season
for season in seasons:

    with open(f"{config.DATA_DIR}/season_courses/{season}.json", "r") as f:
        season_courses = ujson.load(f)

    # track progress for each season
    tqdm.write(f"Fetching class information for season {season}")

    # merge all the JSON results per season
    aggregate_season_json = []

    for course in tqdm(season_courses):

        course_json = fetch_course_json(course["code"], course["crn"], course["srcdb"])

        aggregate_season_json.append(course_json)

    # cache to JSON for entire season
    with open(f"{config.DATA_DIR}/course_json_cache/{season}.json", "w") as f:
        f.write(ujson.dumps(aggregate_season_json, indent=4))

    print()

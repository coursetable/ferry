import argparse

import ujson
from ferry import config
from ferry.includes.class_processing import (
    fetch_course_json,
    fetch_season_courses,
    fetch_seasons,
)
from tqdm import tqdm

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

seasons_latest = False

if args.seasons is not None:
    seasons_latest = len(args.seasons) == 1 and args.seasons[0].startswith("LATEST")

if args.seasons is None or seasons_latest:

    # get sorted list of all available seasons
    seasons = fetch_seasons()

    with open(f"{config.DATA_DIR}/seasons.json", "w") as f:
        f.write(ujson.dumps(seasons, indent=4))

    if args.seasons is None:

        print(f"Fetching all seasons: {seasons}")


    # if fetching latest n seasons, truncate the list
    if seasons_latest:

        num_latest = int(args.seasons[0].split("_")[1])

        seasons = seasons[-num_latest:]

        print(f"Fetching latest {num_latest} seasons: {seasons}")

else:

    seasons = args.seasons

    print(f"Fetching supplied seasons: {seasons}")

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
    pbar = tqdm(total=len(season_courses), ncols=96)
    pbar.set_description(f"Fetching class information for season {season}")

    # merge all the JSON results per season
    aggregate_season_json = []

    for course in season_courses:

        course_json = fetch_course_json(course["code"], course["crn"], course["srcdb"])

        aggregate_season_json.append(course_json)

        pbar.update(1)

    pbar.close()

    # cache to JSON for entire season
    with open(f"{config.DATA_DIR}/course_json_cache/{season}.json", "w") as f:
        f.write(ujson.dumps(aggregate_season_json, indent=4))

    print()

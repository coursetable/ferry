"""
Fetches the following information from the Yale Courses API, in JSON format:

    (1) A list of all courses for each season
        (/api_output/season_courses/)

    (2) Detailed information for each course, for each season
        (/api_output/course_json_cache/)
"""

import argparse

import ujson

from ferry import config
from ferry.crawler.common_args import add_seasons_args, parse_seasons_arg
from ferry.includes.class_processing import fetch_course_json, fetch_season_courses
from ferry.includes.tqdm import tqdm

# -----------------------------------------
# Retrieve courses from unofficial Yale API
# -----------------------------------------

# allow the user to specify seasons (useful for testing and debugging)
parser = argparse.ArgumentParser(description="Fetch classes")
add_seasons_args(parser)

args = parser.parse_args()

# list of seasons previously from fetch_seasons.py
with open(f"{config.DATA_DIR}/course_seasons.json", "r") as f:
    all_viable_seasons = ujson.load(f)

seasons = parse_seasons_arg(args.seasons, all_viable_seasons)

# get lists of classes per season
for season in seasons:
    print(f"Fetching class list for season {season}")

    season_courses = fetch_season_courses(season, criteria=[])

    # cache list of classes
    with open(f"{config.DATA_DIR}/season_courses/{season}.json", "w") as f:
        f.write(ujson.dumps(season_courses, indent=4))


# get lists of classes per season
for season in seasons:
    print(f"Fetching first-year seminars for season {season}")

    season_courses = fetch_season_courses(
        season, criteria=[{"field": "fsem_attrs", "value": "Y"}]
    )

    # cache list of classes
    with open(f"{config.DATA_DIR}/season_courses/{season}_fysem.json", "w") as f:
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

from tqdm import tqdm
import json

from includes.class_processing import *

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

# list of all available seasons
seasons = fetch_seasons()

with open("./api_output/seasons.json", "w") as f:
    f.write(json.dumps(seasons, indent=4))

# get lists of classes per season
for season in seasons:
    print("Fetching class list for season {}".format(season))

    season_courses = fetch_season_courses(season)

    # cache list of classes
    with open("./api_output/season_courses/"+season+".json", "w") as f:
        f.write(json.dumps(season_courses, indent=4))

# fetch detailed info for each class in each season
for season in seasons:

    with open("./api_output/season_courses/"+season+".json", "r") as f:
        season_courses = json.load(f)

    # track progress for each season
    pbar = tqdm(total=len(season_courses), ncols=96)
    pbar.set_description(
        "Fetching class information for season {}".format(season))

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
    with open("./api_output/course_json_cache/{}.json".format(season), "w") as f:
        f.write(json.dumps(aggregate_season_json, indent=4))

    print()

# ---------------------------------------
# Retrieve courses from official Yale API
# ---------------------------------------

# Yale courses API key
with open('./private/yale_api_key.txt', 'r') as f:
    api_key = f.read().strip()

api_seasons = fetch_all_api_seasons()

with open("./api_output/api_seasons.json", "w") as f:
    f.write(json.dumps(api_seasons, indent=4))

# get subjects per season (required for querying classes per season)
for season in api_seasons:
    print("Fetching subjects for season {}".format(season))

    season_courses = []

    season_subjects = fetch_season_subjects(season, api_key)

    # cache list of subjects
    with open("./api_output/api_subjects/"+season+".json", "w") as f:
        f.write(json.dumps(season_subjects, indent=4))

    pbar = tqdm(total=len(season_subjects), ncols=96)
    pbar.set_description("Fetching courses for each subject".format(season))

    for subject in season_subjects:
        subject = subject["code"]

        subject_courses = fetch_season_subject_courses(
            season, subject, api_key)

        season_courses.append(subject_courses)

        pbar.update(1)

    # degroup by subject
    season_courses = [x for y in season_courses for x in y]

    # save courses data
    with open("./api_output/api_course_json_cache/{}.json".format(season), "w") as f:
        f.write(json.dumps(season_courses, indent=4))

    pbar.close()

import argparse
import csv
import datetime
import getpass
import re
import time
from os import listdir
from os.path import isfile, join

import diskcache
import requests
import ujson
from tqdm import tqdm

from ferry import config
from ferry.includes.cas import create_session
from ferry.includes.rating_processing import fetch_course_eval

"""
================================================================
This script fetches course evaluation data from the Yale 
Online Course Evaluation (OCE), in JSON format.
================================================================
"""


class FetchRatingsError(Exception):
    pass


EXCLUDE_SEASONS = [
    "201701",
    "201702",
    "202001",
    "202002",
    "202003",
    "202101",
]

# allow the user to specify seasons
parser = argparse.ArgumentParser(description="Fetch ratings")
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

    print(f"Fetching ratings for all seasons: {seasons}")

else:

    seasons_latest = len(args.seasons) == 1 and args.seasons[0].startswith("LATEST")

    # if fetching latest n seasons, truncate the list and log it
    if seasons_latest:

        num_latest = int(args.seasons[0].split("_")[1])

        seasons = all_viable_seasons[-num_latest:]

        print(f"Fetching ratings for latest {num_latest} seasons: {seasons}")

    # otherwise, use and check the user-supplied seasons
    else:

        # Check to make sure user-inputted seasons are valid
        if all(season in all_viable_seasons for season in args.seasons):

            seasons = args.seasons
            print(f"Fetching ratings for supplied seasons: {seasons}")

        else:
            raise FetchRatingsError("Invalid season.")


# initiate Yale session to access ratings
session = create_session()
print("Cookies: ", session.cookies.get_dict())

# get the list of all course JSON files as previously fetched
season_jsons_path = f"{config.DATA_DIR}/season_courses/"

yale_college_cache = diskcache.Cache(f"{config.DATA_DIR}/yale_college_cache")


@yale_college_cache.memoize()
def is_yale_college(season_code, crn):
    all_params = {
        "other": {"srcdb": season_code},
        "criteria": [{"field": "crn", "value": crn}],
    }
    all_response = requests.post(
        "https://courses.yale.edu/api/?page=fose&route=search",
        data=ujson.dumps(all_params),
    )
    all_data = all_response.json()

    if all_data["count"] < 1:
        # We don't think this even exists, so just attempt it - truthy value.
        return "try it anyways"

    yc_params = {
        "other": {"srcdb": season_code},
        "criteria": [{"field": "crn", "value": crn}, {"field": "col", "value": "YC"}],
    }
    yc_data = requests.post(
        "https://courses.yale.edu/api/?page=fose&route=search&col=YC",
        data=ujson.dumps(yc_params),
    ).json()

    if yc_data["count"] == 0:
        # Not available in Yale College.
        return False

    return True


# get current date to exclude old seasons w/o evaluations
now = datetime.datetime.now()

# load and parse season JSONs
queue = []

for season_code in seasons:

    if season_code in EXCLUDE_SEASONS or int(season_code[:4]) < now.year - 3:
        print(f"Skipping season {season_code}")
        continue

    print(f"Adding season {season_code}")

    with open(season_jsons_path + season_code + ".json", "r") as f:
        season_json = ujson.load(f)

    for course in season_json:  # Loop through each course in the season

        queue.append((season_code, course["crn"]))

# with open(f"{config.DATA_DIR}/listings_with_extra_info.csv", "r") as csvfile:
#     reader = csv.reader(csvfile)
#     for _, _, _, _, _, _, season_code, crn, extra_info in reader:
#         if "Cancelled" in extra_info:
#             continue
#         if season_code in ["201903", "201901", "201803", "201801", "201703"]:
#             queue.append((season_code, crn))

# queue = [
#     ("201903", "11970"),  # basic test
#     ("201703", "10738"),  # no evaluations available
#     ("201703", "13958"),  # DRAM class?
#     ("201703", "10421"),  # APHY 990 (class not in Yale College)
#     ("201703", "16119"),  # no evaluations available (doesn't show in OCE)
# ]

for season_code, crn in tqdm(queue):
    course_unique_id = f"{season_code}-{crn}"
    output_path = f"{config.DATA_DIR}/course_evals/{course_unique_id}.json"

    if isfile(output_path):
        # tqdm.write(f"Skipping course {course_unique_id} - already exists")
        continue

    if not is_yale_college(season_code, crn):
        # tqdm.write("skipping - not in yale college")
        continue

    tqdm.write(f"Working on {course_unique_id} ... ")
    tqdm.write("                            ", end="")

    try:
        session = create_session()
        course_eval = fetch_course_eval(session, crn, season_code)

        with open(output_path, "w") as f:
            f.write(ujson.dumps(course_eval, indent=4))

        tqdm.write(f"dumped in JSON")
    # except SeasonMissingEvalsError:
    #     tqdm.write(f"Skipping season {season_code} - missing evals")
    # except CrawlerError:
    #     tqdm.write(f"skipped - missing evals")
    except Exception as e:
        import traceback

        # traceback.print_exc()
        tqdm.write(f"skipped - unknown error {e}")

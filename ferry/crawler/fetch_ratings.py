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
from ferry import config
from ferry.includes.cas import create_session
from ferry.includes.rating_processing import *
from tqdm import tqdm

"""
================================================================
This script fetches course evaluation data from the Yale 
Online Course Evaluation (OCE), in JSON format.
================================================================
"""

session = create_session()
print("Cookies: ", session.cookies.get_dict())

# get the list of all course JSON files as previously fetched
season_jsons_path = f"{config.DATA_DIR}/season_courses/"

# list of seasons previously from fetch_seasons.py
with open(f"{config.DATA_DIR}/course_seasons.json", "r") as f:
    seasons = ujson.loads(f.read())

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


now = datetime.datetime.now()

# load and parse season JSONs
queue = []

for season_code in seasons:

    if (
        season_code == "202001"
        or season_code == "202002"
        or season_code == "201701"
        or season_code == "201702"  # spring/summer 2017 is no longer available
        or int(season_code[:4]) < now.year - 3
    ):
        continue

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

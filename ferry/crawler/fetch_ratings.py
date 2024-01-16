"""
This script fetches course evaluation data from the Yale
Online Course Evaluation (OCE), in JSON format through the
following steps:

    1. Selection of seasons to fetch ratings
    2. Construction of a cached check for Yale College courses
    3. Aggregation of all season courses into a queue
    4. Fetch and save OCE data for each course in the queue

"""
import argparse
import datetime
import os
from os.path import isfile
from typing import Union

import diskcache
import requests
import ujson

from ferry import config
from ferry.crawler.common_args import add_seasons_args, parse_seasons_arg
from ferry.includes.cas import create_session, _create_session_from_cookie
from ferry.includes.rating_processing import fetch_course_eval
from ferry.includes.tqdm import tqdm


class FetchRatingsError(Exception):
    """
    Error object for fetch ratings exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


EXCLUDE_SEASONS = [
    "201701",  # too old
    "201702",  # too old
    "201703",  # too old
    "201801",  # too old
    "201901",  # too old
    "201902",  # too old
    "201903",  # too old
    "202001",  # not evaluated because of COVID
    "202002",  # too old
    "202003",
    "202101",
]

# allow the user to specify seasons
parser = argparse.ArgumentParser(description="Fetch ratings")
add_seasons_args(parser)

parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    help="force overwrite existing evaluation outputs",
)

args = parser.parse_args()

# --------------------------------------------------------
# Load all seasons and compare with selection if specified
# --------------------------------------------------------

# list of seasons previously from fetch_seasons.py
with open(f"{config.DATA_DIR}/course_seasons.json", "r") as f:
    all_viable_seasons = ujson.load(f)

seasons = parse_seasons_arg(args.seasons, all_viable_seasons)


yale_college_cache = diskcache.Cache(f"{config.DATA_DIR}/yale_college_cache")


@yale_college_cache.memoize()
def is_yale_college(course_season_code: str, course_crn: str) -> Union[str, bool]:

    """
    Helper function to check if course is in Yale College
    (only Yale College and Summer Session courses are rated)
    """

    all_params = {
        "other": {"srcdb": course_season_code},
        "criteria": [{"field": "crn", "value": course_crn}],
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
        "other": {"srcdb": course_season_code},
        "criteria": [
            {"field": "crn", "value": course_crn},
            {"field": "col", "value": "YC"},
        ],
    }
    yc_data = requests.post(
        "https://courses.yale.edu/api/?page=fose&route=search&col=YC",
        data=ujson.dumps(yc_params),
    ).json()

    if yc_data["count"] == 0:
        # Not available in Yale College.
        return False

    return True


# -----------------------------------
# Queue courses to query from seasons
# -----------------------------------

# Test cases----------------------------------------------------------------
# queue = [
#     ("201903", "11970"),  # basic test
#     ("201703", "10738"),  # no evaluations available
#     ("201703", "13958"),  # DRAM class?
#     ("201703", "10421"),  # APHY 990 (class not in Yale College)
#     ("201703", "16119"),  # no evaluations available (doesn't show in OCE)
#     ("201802", "30348"),  # summer session course
# ]
# --------------------------------------------------------------------------

# get the list of all course JSON files as previously fetched
season_jsons_path = f"{config.DATA_DIR}/season_courses/"

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

# -------------------------------
# Fetch course ratings from queue
# -------------------------------

# initiate Yale session to access ratings
#session = create_session()
# try new way
with open('cascookie.txt', 'r') as cookiefile:
        castgc = cookiefile.read()
        session = _create_session_from_cookie(castgc)

for season_code, crn in tqdm(queue):
    # Create directory if doesn't exist
    output_dir = f"{config.DATA_DIR}/course_evals/"
    if os.path.exists(output_dir) is False:
        os.makedirs(output_dir)

    course_unique_id = f"{season_code}-{crn}"
    output_path = f"{config.DATA_DIR}/course_evals/{course_unique_id}.json"

    if isfile(output_path) and not args.force:
        # tqdm.write(f"Skipping course {course_unique_id} - already exists")
        continue

    if season_code[-2:] != "02" and not is_yale_college(season_code, crn):
        # tqdm.write("skipping - not in yale college")
        continue

    tqdm.write(f"Working on {course_unique_id} ... ")
    tqdm.write("                            ", end="")

    try:
        course_eval = fetch_course_eval(session, crn, season_code)

        with open(output_path, "w+") as f:
            f.write(ujson.dumps(course_eval, indent=4))

        tqdm.write("dumped in JSON")
    # except SeasonMissingEvalsError:
    #     tqdm.write(f"Skipping season {season_code} - missing evals")
    # except CrawlerError:
    #     tqdm.write(f"skipped - missing evals")

    # pylint: disable=broad-except
    except Exception as error:

        # traceback.print_exc()
        tqdm.write(f"skipped - unknown error {error}")

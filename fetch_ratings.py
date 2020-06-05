import requests
import getpass
import json
import time
import re
import sys
import csv
from tqdm import tqdm
from includes.class_processing import fetch_seasons
from includes.cas import create_session_from_cookie, create_session_from_credentials
from includes.rating_processing import *

from os import listdir
from os.path import isfile, join

"""
================================================================
This script fetches course evaluation data from the Yale 
Online Course Evaluation (OCE), in JSON format.
================================================================
"""

# Input NetID and password to login to Yale CAS
# netid = input("Yale NetID: ")
# password = getpass.getpass()
# netid = "hks24"
# with open("./private/netid.txt", "r") as passwordfile:
#     password = passwordfile.read().strip()
# session = create_session_from_credentials(netid, password)

# Login
with open("./private/cascookie.txt", "r") as cookiefile:
    castgc = cookiefile.read().strip()
session = create_session_from_cookie(castgc)
print("Cookies: ", session.cookies.get_dict())

# get the list of all course JSON files as previously fetched
season_jsons_path = "./api_output/courses/"

seasons = fetch_seasons()

# load and parse season JSONs
# for season_code in seasons:
#     if season_code == '202001' or season_code == '202002':
#         continue
#     with open(season_jsons_path + season_code + ".json", "r") as f:
#         season_json = json.load(f)

#     for course in season_json: # Loop through each course in the season

queue = []
with open("./api_output/listings.csv", "r") as csvfile:
    reader = csv.reader(csvfile)
    for _, _, _, _, _, _, season_code, crn in reader:
        if season_code in ["201903", "201901", "201803", "201801", "201703"]:
            queue.append((season_code, crn))

# queue = [
#     ("201903", "11970"),  # basic test
#     ("201703", "10738"),  # no evaluations available
#     ("201703", "13958"),
# ]

for season_code, crn in tqdm(queue):
    course_unique_id = f"{season_code}-{crn}"
    output_path = "./api_output/course_evals/{}.json".format(course_unique_id)

    if isfile(output_path):
        tqdm.write(f"Skipping course {course_unique_id} - already exists")
        continue

    tqdm.write(f"Working on {course_unique_id} ... ")
    tqdm.write("                            ", end="")
    try:
        session = create_session_from_cookie(castgc)
        course_eval = fetch_course_eval(session, crn, season_code)

        with open(output_path, "w") as f:
            f.write(json.dumps(course_eval, indent=4))

        tqdm.write(f"dumped in JSON")
    # except SeasonMissingEvalsError:
    #     tqdm.write(f"Skipping season {season_code} - missing evals")
    # except CrawlerError:
    #     tqdm.write(f"skipped - missing evals")
    except Exception as e:
        import traceback

        traceback.print_exc()
        tqdm.write(f"skipped - unknown error {e}")

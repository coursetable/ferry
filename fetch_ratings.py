import requests
import getpass 
import json
import time
import re
import sys
from tqdm import tqdm
from includes.class_processing import fetch_seasons
from includes.cas import create_session_from_cookie
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

# Login
with open('./private/cascookie.txt', 'r') as cookiefile:
    castgc = cookiefile.read()
session = create_session_from_cookie(castgc)
print("Cookies: ", session.cookies.get_dict())

# get the list of all course JSON files as previously fetched
season_jsons_path = "./api_output/season_courses/"

seasons = fetch_seasons()

# load and parse season JSONs
for season_code in seasons:
    if season_code == '202001' or season_code == '202002':
        continue
    with open(season_jsons_path + season_code + ".json", "r") as f:
        season_json = json.load(f)

    for season_course in season_json: # Loop through each course in the season
        course_unique_id = season_course["code"] + "-" + \
            season_course["crn"] + "-" + season_course["srcdb"]
        output_path = "./api_output/course_evals/{}.json".format(course_unique_id)
        """
        if isfile(output_path):
            print("Evaluations for course:",course_unique_id,"already exists")
            continue
        """

        try:
            course_eval , season_has_eval = fetch_course_eval(session,season_course['crn'],season_code)

            if (season_has_eval == -1):
                raise seasonMissingEvalsError
            
            with open(output_path, "w") as f:
                f.write(json.dumps(course_eval, indent=4))

            print("Evaluations for course:",course_unique_id,"dumped in JSON")
        except seasonMissingEvalsError:
            print(f"Skipping season {season_code} - missing evals")
            break
        except CourseMissingEvalsError:
            print(f"Skipping course {course_unique_id} - missing evals")

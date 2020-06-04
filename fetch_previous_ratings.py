import requests
import getpass 
import json
import time
import re
import sys
from tqdm import tqdm
from includes.class_processing import fetch_seasons
from includes.cas import create_session_from_cookie, create_session_from_credentials
from includes.rating_processing import CourseMissingEvalsError

from os import listdir
from os.path import isfile, join

"""
================================================================
This script fetches evaluations data from coursetable.com and converts
the JSON formatting into something compatible with the generic
evaluations crawler in fetch_ratings.py.
================================================================
"""

def fetch_legacy_ratings(session: requests.Session, legacy_course_id: str, season: str, crn: str):
    # We need the crn to associate the evaluation entry
    # with the right crn, since a legacy_course_id can
    # represent multiple instances of a cross-listing. This
    # duplication will be reversed when we merge the ratings
    # back into the system.
    
    url = f"https://coursetable.com/GetEvaluations.php?evaluationIds[]={legacy_course_id}"

    response = session.get(url)
    if response.status_code != 200:
        raise CourseMissingEvalsError

    full_json = response.json()
    if not full_json["success"]:
        raise CourseMissingEvalsError

    data = full_json["data"][legacy_course_id]

    return {
        "crn_code": crn,
        "season": season,
        "enrollment": {
            "enrolled": int(data["enrollment"]),
            "responses": None,
            "declined": None,
            "no response": None,
        },
        "ratings": [
            {
                "question_id": "YC402",
                "question_text": "Your level of engagement with the course was:",
                "options": [ "very low", "low", "medium", "high", "very high" ],
                "data": data["ratings"]["engagement"],
            },
            {
                "question_id": "YC404",
                "question_text": "What is your overall assessment of this course?",
                "options": [ "poor", "fair", "good", "very good", "excellent" ],
                "data": data["ratings"]["rating"],
            },
            {
                "question_id": "YC405",
                "question_text": "The course was well organized to facilitate student learning.",
                "options": [ "strongly disagree", "disagree", "neutral", "agree", "strongly agree" ],
                "data": data["ratings"]["organization"],
            },
            {
                "question_id": "YC406",
                "question_text": "I received clear feedback that improved my learning.",
                "options": [ "strongly disagree", "disagree", "neutral", "agree", "strongly agree" ],
                "data": data["ratings"]["feedback"],
            },
            {
                "question_id": "YC407",
                "question_text": "Relative to other courses you have taken at Yale, the level of <u>intellectual challenge</u> of this course was:",
                "options": [ "much less", "less", "same", "greater", "much greater" ],
                "data": data["ratings"]["challenge"],
            },
            {
                "question_id": "YC408",
                "question_text": "Relative to other courses you have taken at Yale, the <u>workload</u> of this course was:",
                "options": [ "much less", "less", "same", "greater", "much greater" ],
                "data": data["ratings"]["workload"],
            },
        ],
        "narratives": [
            {
                "question_id": "YC401",
                "question_text": "What knowledge, skills, and insights did you develop by taking this course?",
                "comments": data["comments"]["knowledge"],
            },
            {
                "question_id": "YC403",
                "question_text": "What are the strengths and weaknesses of this course and how could it be improved?",
                "comments": data["comments"]["strengthsWeaknesses"],
            },
            {
                "question_id": "YC409",
                "question_text": "Would you recommend this course to another student? Please explain.",
                "comments": data["comments"]["recommend"],
            },
        ],
        # "raw": data,
    }


if __name__ == '__main__':
    # Input NetID and password to login to Yale CAS
    # netid = input("Yale NetID: ")
    # password = getpass.getpass()
    netid = 'hks24'
    with open('./private/netid.txt', 'r') as passwordfile:
        password = passwordfile.read().strip()
    session = create_session_from_credentials(netid, password)

    # Login
    # with open('./private/cascookie.txt', 'r') as cookiefile:
    #     castgc = cookiefile.read().strip()
    # session = create_session_from_cookie(castgc)
    print("Cookies: ", session.cookies.get_dict())

    # Test with ACCT 270 from 201903.
    prev = [
        ["52358", "201903", "11970"],
    ]

    for legacy_course_id, season, crn in prev:
        output_path = f"./api_output/previous_evals/{season}-{crn}.json"
        course_eval = fetch_legacy_ratings(session, legacy_course_id, season, crn)

        with open(output_path, "w") as f:
            f.write(json.dumps(course_eval, indent=4))

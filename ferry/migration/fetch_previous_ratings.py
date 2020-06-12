import requests
import collections
import getpass
import ujson
import time
import re

import sys

sys.path.append("..")

from tqdm import tqdm
from includes.class_processing import fetch_seasons
from includes.cas import create_session_from_cookie, create_session_from_credentials
from includes.rating_processing import CourseMissingEvalsError
from private import extract_db

from os import listdir
from os.path import isfile, join


"""
================================================================
This script fetches evaluations data from the coursetable.com
database and yields a JSON format similar to the one produced
by the fetch_ratings.py script.

To use this script, you'll need to create an extract_db.py file
in the private directory, and it should contain a function get_db()
which returns a handle to a MySQL database using pymysql. It would
be preferable if this handle were read-only. An example:

    # private/extract_db.py (sample)
    import pymysql

    def get_db(db: str):
        connection = pymysql.connect(host='localhost',
                                    user='sample',
                                    password='sample',
                                    db=db,
                                    cursorclass=pymysql.cursors.DictCursor)
        return connection

================================================================
"""


def fetch_course_lists(db, limit=None):
    limit_string = ""
    if limit:
        limit_string = f"LIMIT {limit}"

    """
    # Fetch from `course_names`.
    with db.cursor() as cursor:
        sql = f"SELECT * FROM `course_names` {limit_string}"
        cursor.execute(sql)
        course_names = cursor.fetchall()
    """

    # Fetch from `evaluation_course_names`.
    with db.cursor() as cursor:
        sql = f"SELECT * FROM `evaluation_course_names` {limit_string}"
        cursor.execute(sql)
        raw_courses = cursor.fetchall()

    listings = []
    for c in raw_courses:
        listings.append(
            (
                str(c["season"]),
                str(c["crn"]),
                {
                    "subject": c["subject"],
                    "number": c["number"],
                    "section": c["section"],
                },
            )
        )

    return listings


def fetch_legacy_ratings(db, season: str, crn: str, extras: dict):
    # Fetch Coursetable course_id.
    with db.cursor() as cursor:
        sql = "SELECT `course_id` FROM `evaluation_course_names` WHERE `season` = %s AND `crn` = %s LIMIT 1"
        cursor.execute(sql, (season, crn))
        course_id = cursor.fetchone()["course_id"]

    # Enrollment data.
    with db.cursor() as cursor:
        sql = "SELECT `enrollment` FROM `evaluation_courses` WHERE `id` =  %s LIMIT 1"
        cursor.execute(sql, (course_id,))
        enrollment = cursor.fetchone()["enrollment"]

    # Narrative comments.
    with db.cursor() as cursor:
        sql = "SELECT `question_id`, `comment` FROM `evaluation_comments` WHERE `course_id` = %s"
        cursor.execute(sql, (course_id,))
        data = cursor.fetchall()

    narrative_comments = collections.defaultdict(lambda: [])
    for item in data:
        narrative_comments[item["question_id"]].append(item["comment"])

    # Ratings.
    with db.cursor() as cursor:
        sql = "SELECT `question_id`, `counts` FROM `evaluation_ratings` WHERE `course_id` = %s"
        cursor.execute(sql, (course_id,))
        data = cursor.fetchall()

    ratings_data = dict()
    for item in data:
        ratings_data[item["question_id"]] = ujson.loads(item["counts"])

    # Checks
    questions_ids = tuple(narrative_comments.keys()) + tuple(ratings_data.keys())
    # if not questions_ids:
    # raise CourseMissingEvalsError

    # Question statements.
    if questions_ids:
        with db.cursor() as cursor:
            where_clause = ",".join(["%s"] * len(questions_ids))
            sql = f"SELECT `id`, `text`, `options` FROM `evaluation_questions` WHERE `id` IN ({where_clause})"
            cursor.execute(sql, tuple(questions_ids))
            data = cursor.fetchall()

        questions = dict()
        for item in data:
            questions[item["id"]] = (item["text"], item["options"])
    else:
        extras["note"] = "no evaluations in database"

    return {
        "crn_code": crn,
        "season": season,
        "legacy_coursetable_course_id": course_id,
        "enrollment": {
            "enrolled": enrollment,
            "responses": None,
            "declined": None,
            "no response": None,
        },
        "ratings": [
            {
                "question_id": question_id,
                "question_text": questions[question_id][0],
                "options": ujson.loads(questions[question_id][1]),
                "data": data,
            }
            for question_id, data in ratings_data.items()
        ],
        "narratives": [
            {
                "question_id": question_id,
                "question_text": questions[question_id][0],
                "comments": comments,
            }
            for question_id, comments in narrative_comments.items()
        ],
        # The extras are associated specifically with this CRN, and does not
        # take into account any cross-listing.
        "extras": extras,
    }


if __name__ == "__main__":
    db = extract_db.get_db("yale_advanced_oci")

    prev = fetch_course_lists(db, limit=None)

    """
    prev = [
        # Test with ACCT 270 from 201903.
        ("201903", "11970", {}),
        # Test with ACCT 170 from 200903.
        ("200903", "11256", {}),
        # Test with ECON 466 01 from 201003. Compare with https://dougmckee.net/aging-evals-fall-2010.pdf.
        ("201003", "12089", {}),
    ]
    """

    prev = list(reversed(prev))
    for season, crn, extras in tqdm(prev):
        identifier = f"{season}-{crn}"

        output_path = f"../api_output/previous_evals/{identifier}.json"
        if isfile(output_path):
            tqdm.write(f"Skipping {identifier} - already exists")
            continue

        try:
            tqdm.write(f"Processing {identifier}")
            course_eval = fetch_legacy_ratings(db, season, crn, extras)

            with open(output_path, "w") as f:
                f.write(ujson.dumps(course_eval, indent=4))
        except KeyError as e:
            # Some courses produce YC402-YCWR and similar question IDs for ratings data. The new importer can handle this.
            if season == "201903" or season == "201901":
                tqdm.write(f"Failed - blacklist {identifier}")
            else:
                raise CourseMissingEvalsError from e
        except CourseMissingEvalsError:
            tqdm.write(f"Failed to fetch {identifier}")

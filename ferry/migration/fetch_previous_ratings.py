"""
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

(Note that as of October 2020, this script no longer works as we have upgraded
the main site. However, the data files it produces are archived in our
ferry-data repository. If you are not part of the CourseTable team and are
interested in accessing these data, please contact us.)
"""

import collections
from os.path import isfile

import ujson

from ferry import config
from ferry.includes.rating_processing import CrawlerError
from ferry.includes.tqdm import tqdm
from private import extract_db


def fetch_course_lists(db_connection, limit=None):
    """
    Fetch list of courses with evaluations.
    """

    limit_string = ""
    if limit:
        limit_string = f"LIMIT {limit}"

    # Fetch from `course_names`.
    # with db_connection.cursor() as cursor:
    #    sql = f"SELECT * FROM `course_names` {limit_string}"
    #    cursor.execute(sql)
    #    course_names = cursor.fetchall()

    # Fetch from `evaluation_course_names`.
    with db_connection.cursor() as cursor:
        sql = f"SELECT * FROM `evaluation_course_names` {limit_string}"
        cursor.execute(sql)
        raw_courses = cursor.fetchall()

    listings = []
    for course in raw_courses:
        listings.append(
            (
                str(course["season"]),
                str(course["crn"]),
                {
                    "subject": course["subject"],
                    "number": course["number"],
                    "section": course["section"],
                },
            )
        )

    return listings


def fetch_legacy_ratings(db_connection, season: str, crn: str, extras: dict):
    """
    Fetch ratings for a given season and CRN.
    """

    # Fetch Coursetable course_id.
    with db_connection.cursor() as cursor:
        sql = """SELECT `course_id` FROM `evaluation_course_names`
                 WHERE `season` = %s AND `crn` = %s LIMIT 1"""
        cursor.execute(sql, (season, crn))
        course_id = cursor.fetchone()["course_id"]

    # Enrollment data.
    with db_connection.cursor() as cursor:
        sql = """SELECT `enrollment` FROM `evaluation_courses`
                 WHERE `id` =  %s LIMIT 1"""
        cursor.execute(sql, (course_id,))
        enrollment = cursor.fetchone()["enrollment"]

    # Narrative comments.
    with db_connection.cursor() as cursor:
        sql = """SELECT `question_id`, `comment` FROM `evaluation_comments`
                 WHERE `course_id` = %s"""
        cursor.execute(sql, (course_id,))
        data = cursor.fetchall()

    narrative_comments = collections.defaultdict(lambda: [])
    for item in data:
        narrative_comments[item["question_id"]].append(item["comment"])

    # Ratings.
    with db_connection.cursor() as cursor:
        sql = """SELECT `question_id`, `counts` FROM `evaluation_ratings`
                 WHERE `course_id` = %s"""
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
        with db_connection.cursor() as cursor:
            where_clause = ",".join(["%s"] * len(questions_ids))
            sql = f"""SELECT `id`, `text`, `options` FROM `evaluation_questions`
                      WHERE `id` IN ({where_clause})"""
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
    connection = extract_db.get_db("yale_advanced_oci")

    prev = fetch_course_lists(connection, limit=None)

    """
    prev = [
        # Test with ACCT 270 from 201903.
        ("201903", "11970", {}),
        # Test with ACCT 170 from 200903.
        ("200903", "11256", {}),
        # Test with ECON 466 01 from 201003.
        # Compare with https://dougmckee.net/aging-evals-fall-2010.pdf.
        ("201003", "12089", {}),
    ]
    """

    prev = list(reversed(prev))
    for course_season, course_crn, course_extras in tqdm(prev):
        identifier = f"{course_season}-{course_crn}"

        output_path = f"{config.DATA_DIR}/previous_evals/{identifier}.json"
        if isfile(output_path):
            tqdm.write(f"Skipping {identifier} - already exists")
            continue

        try:
            tqdm.write(f"Processing {identifier}")
            course_eval = fetch_legacy_ratings(
                connection, course_season, course_crn, course_extras
            )

            with open(output_path, "w") as file:
                file.write(ujson.dumps(course_eval, indent=4))
        except KeyError as err:
            # Some courses produce YC402-YCWR and similar question IDs for ratings data.
            # The new importer can handle this.
            if course_season in ("201903", "201901"):
                tqdm.write(f"Failed - blacklist {identifier}")
            else:
                raise CrawlerError from err
        except CrawlerError:
            tqdm.write(f"Failed to fetch {identifier}")

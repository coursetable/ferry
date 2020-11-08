import argparse
import os

import ujson

from ferry import config
from ferry.includes.class_parsing import extract_flags
from ferry.includes.migration import (
    convert_old_description,
    convert_old_meetings,
    convert_old_time,
)
from ferry.includes.tqdm import tqdm
from ferry.includes.utils import *

"""
================================================================
This script migrates the old CourseTable JSON files to the
new format used for the course JSONs.
================================================================
"""

parser = argparse.ArgumentParser(description="Import classes")
parser.add_argument(
    "-s",
    "--seasons",
    nargs="+",
    help="seasons to import",
    default=None,
    required=False,
)

args = parser.parse_args()
seasons = args.seasons

if seasons is None:
    seasons = [
        filename.split(".")[0]
        for filename in os.listdir(f"{config.DATA_DIR}/previous_json/")
        if filename[-4:] == "json" and filename[:5] != "evals"
    ]

    seasons = sorted(seasons)

for season in seasons:

    migrated_courses = []

    with open(f"{config.DATA_DIR}/previous_json/{season}.json", "r") as f:

        previous_json = ujson.load(f)

        tqdm.write(f"Processing courses in season {season}")
        for course in tqdm(previous_json):

            migrated_course = dict()

            migrated_course["season_code"] = str(season)

            migrated_course["description"] = convert_old_description(
                course["description"]
            )
            migrated_course["requirements"] = course["requirements"]

            def truncate_title(x):
                return f"{x[:29]}..." if len(x) > 32 else x

            migrated_course["short_title"] = truncate_title(course["long_title"])

            migrated_course["title"] = course["long_title"]

            extra_info_map = {
                "Cancelled": "CANCELLED",
                "": "ACTIVE",
                "Moved to spring term": "MOVED_TO_SPRING_TERM",
                "Number changed-See description": "NUMBER_CHANGED",
                "Moved to preceding fall term": "MOVED_TO_FALL_TERM",
                "The": "ACTIVE",  # ? no idea what this means, so assuming active
                "Closed to further enrollments": "CLOSED",
            }

            migrated_course["extra_info"] = extra_info_map[course["extra_info"]]
            migrated_course["professors"] = sorted(course["professors"])

            migrated_course["crn"] = course["oci_id"]
            migrated_course["crns"] = course["oci_ids"]

            migrated_course["subject"] = course["subject"]
            migrated_course["number"] = str(course["number"])
            migrated_course[
                "course_code"
            ] = f"{course['subject']} {migrated_course['number']}"

            migrated_course["section"] = course["section"]

            (
                migrated_course["times_summary"],
                migrated_course["times_long_summary"],
                migrated_course["times_by_day"],
            ) = convert_old_meetings(course["times"])

            migrated_course["locations_summary"] = course["locations_summary"]
            if migrated_course["locations_summary"] == "":
                migrated_course["locations_summary"] = "TBA"

            migrated_course["skills"] = sorted(course["skills"])
            migrated_course["areas"] = sorted(course["areas"])
            migrated_course["course_home_url"] = course["course_home_url"]
            migrated_course["syllabus_url"] = course["syllabus_url"]

            migrated_course["flags"] = extract_flags("".join(course["flags"]))

            migrated_courses.append(migrated_course)

    with open(f"{config.DATA_DIR}/migrated_courses/{season}.json", "w") as f:
        f.write(ujson.dumps(migrated_courses))

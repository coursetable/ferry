import argparse

import ujson
from tqdm import tqdm

from ferry import config
from ferry.includes.class_processing import *
from ferry.includes.migration import *
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
    seasons = fetch_previous_seasons()

for season in seasons:

    migrated_courses = []

    with open(f"{config.DATA_DIR}/previous_json/{season}.json", "r") as f:

        previous_json = ujson.load(f)

        for course in tqdm(
            previous_json, desc=f"Processing courses in season {season}"
        ):

            migrated_course = dict()

            migrated_course["season_code"] = str(season)

            migrated_course["description"] = convert_old_description(course["description"])
            migrated_course["requirements"] = course["requirements"]
            migrated_course["short_title"] = course["title"]
            migrated_course["title"] = course["long_title"]

            migrated_course["extra_info"] = course["extra_info"]
            migrated_course["professors"] = course["professors"]

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

            migrated_course["skills"] = course["skills"]
            migrated_course["areas"] = course["areas"]
            migrated_course["course_home_url"] = course["course_home_url"]
            migrated_course["syllabus_url"] = course["syllabus_url"]

            migrated_course["flags"] = extract_flags("".join(course["flags"]))

            migrated_courses.append(migrated_course)

    with open(f"{config.DATA_DIR}/migrated_courses/{season}.json", "w") as f:
        f.write(ujson.dumps(migrated_courses))

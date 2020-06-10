from tqdm import tqdm
import json

from os import listdir
from os.path import isfile, join

import argparse

import database

"""
================================================================
This script imports the parsed course and evaluation data into the database.
It creates or updates the tables as necessary, so this script is idempotent.
This script does not recalculate any computed values in the schema.
================================================================
"""


def import_class(session, course_info):
    # Create season.
    season, _ = database.get_or_create(
        session, database.Season, season_code=course_info["season_code"],
    )

    # Find or create appropriate listing and course.
    listing = (
        session.query(database.Listing)
        .filter_by(season=season, crn=course_info["crn"])
        .one_or_none()
    )
    if not listing:
        # Check cross-listings for a potential course_id.
        crns = course_info["crns"]
        course_entry = (
            session.query(database.Listing.course_id)
            .filter(database.Listing.season == season)
            .filter(database.Listing.crn.in_(crns))
            .first()
        )

        if not course_entry:
            course = database.Course()
            session.add(course)
        else:
            course = (
                session.query(database.Course)
                .filter_by(course_id=course_entry.course_id)
                .one()
            )

        listing = database.Listing(season=season, crn=course_info["crn"])
        listing.course = course
        session.add(listing)

    # Update listing.
    listing.subject = course_info["subject"]
    listing.number = course_info["number"]
    listing.section = course_info["section"]

    # Resolve professors.
    def resolve_professor(name):
        professor, _ = database.get_or_create(session, database.Professor, name=name)
        return professor

    # Populate course info.
    course = listing.course  # type: database.Course
    course.season = season
    course.areas = course_info["areas"]
    course.course_home_url = course_info["course_home_url"]
    course.description = course_info["description"]
    course.extra_info = course_info["extra_info"]
    course.locations_summary = course_info["locations_summary"]
    course.requirements = course_info["requirements"]
    course.section = course_info["section"]
    course.times_long_summary = course_info["times_long_summary"]
    course.times_summary = course_info["times_summary"]
    course.times_by_day = course_info["times_by_day"]
    course.short_title = course_info["short_title"]
    course.skills = course_info["skills"]
    course.syllabus_url = course_info["syllabus_url"]
    course.title = course_info["title"]
    course.professors = [
        resolve_professor(prof_name) for prof_name in course_info["professors"]
    ]
    # TODO: course.location_times = course_info["TODO"]


if __name__ == "__main__":
    # allow the user to specify seasons (useful for testing and debugging)
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
        # get the list of all course JSON files as previously fetched
        with open("./api_output/seasons.json", "r") as f:
            seasons = json.load(f)

    for season in seasons:
        with open(f"./api_output/parsed_courses/{season}.json", "r") as f:
            parsed_course_info = json.load(f)

        for course_info in parsed_course_info:
            with database.session_scope(database.Session) as session:
                print(f"Importing {course_info}")
                import_class(session, course_info)

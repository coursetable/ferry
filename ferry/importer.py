import argparse
import os
from pathlib import Path

import textdistance
import ujson

from ferry import config, database
from ferry.includes.tqdm import tqdm

"""
================================================================
This script imports the parsed course and evaluation data into the database.
It creates or updates the tables as necessary, so this script is idempotent.
This script does not recalculate any computed values in the schema.
================================================================
"""


def import_course(session, course_info):
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
    listing.course_code = f"{listing.subject} {listing.number}"
    listing.section = course_info["section"]

    # Resolve professors.
    def resolve_professor(name, email, ocs_id):
        professor, professor_new = database.get_or_create(
            session, database.Professor, name=name
        )

        # if we just added a new professor, set the email+ocs_id as well
        if professor_new:
            professor.email = email
            professor.ocs_id = ocs_id

            return professor

        # otherwise, if the professor already exists, update email and ID if nonempty
        if professor.email != email and email != "":

            # if the old email is nonempty, log the change
            if professor.email != "":
                print(
                    f"Updated email for professor '{name}': '{professor.email}'->'{email}'"
                )

            professor.email = email

        if professor.ocs_id != ocs_id and ocs_id != "":

            # if the old OCS ID is nonempty, log the change
            if professor.ocs_id != "":
                print(
                    f"Updated OCS ID for professor '{name}': '{professor.ocs_id}'->'{ocs_id}'"
                )

            professor.ocs_id = ocs_id

        return professor

    # Populate course info.
    course = listing.course  # type: database.Course
    course.season = season
    course.areas = course_info["areas"]
    course.course_home_url = course_info["course_home_url"]
    course.description = course_info["description"]
    course.school = course_info.get("school", None)
    course.credits = course_info.get("credits", None)
    course.extra_info = course_info["extra_info"]
    course.locations_summary = course_info["locations_summary"]
    course.requirements = course_info["requirements"]
    course.times_long_summary = course_info["times_long_summary"]
    course.times_summary = course_info["times_summary"]
    course.times_by_day = course_info["times_by_day"]
    course.short_title = course_info["short_title"]
    course.skills = course_info["skills"]
    course.syllabus_url = course_info["syllabus_url"]
    course.title = course_info["title"]

    course_professors = []

    # if professor emails and ids provided, add them in
    if "professor_emails" in course_info and "professor_ids" in course_info:

        for professor, professor_email, professor_id in zip(
            course_info["professors"],
            course_info["professor_emails"],
            course_info["professor_ids"],
        ):

            course_professors.append(
                resolve_professor(professor, professor_email, professor_id)
            )

    # otherwise (for instance, when importing from migrated legacy files, only add the name)
    else:

        for professor in course_info["professors"]:

            course_professors.append(resolve_professor(professor, "", ""))

    course.professors = course_professors

    # TODO: course.location_times = course_info["TODO"]


def import_demand(session, season, demand_info):

    # Find the associated course.
    possible_course_ids = (
        session.query(
            database.Listing.course_code,
            database.Listing.course_id,
            database.Listing.section,
        ).filter(database.Listing.course_code.in_(demand_info["codes"]))
    ).all()

    unique_course_ids = set(listing[1] for listing in possible_course_ids)

    if len(unique_course_ids) < 1:
        print(
            f"Could not find a course matching {demand_info['codes']} in season {season}"
        )
        return

    demand_info["overall_demand"] = {
        date: int(count) for date, count in demand_info["overall_demand"].items()
    }

    sorted_demand = list(demand_info["overall_demand"].items())

    def date_to_int(date):
        month, day = date.split("/")

        month = int(month)
        day = int(day)

        return month * 100 + day

    sorted_demand.sort(key=lambda x: date_to_int(x[0]))

    latest_demand_date, latest_demand = sorted_demand[-1]

    # allow multiple matching course IDs due to different sections for now
    for course_id in list(unique_course_ids):

        # Set demand information.
        demand_stats, _ = database.get_or_create(
            session, database.DemandStatistics, course_id=course_id
        )

        demand_stats.demand = demand_info["overall_demand"]
        demand_stats.latest_demand = latest_demand
        demand_stats.latest_demand_date = latest_demand_date


def import_evaluation(session, evaluation):
    season_code = str(evaluation["season"])
    crn = str(evaluation["crn_code"])

    course = (
        session.query(database.Course)
        .filter(database.Listing.season_code == season_code)
        .filter(database.Listing.crn == crn)
        .filter(database.Listing.course_id == database.Course.course_id)
        .one_or_none()
    )
    if not course:
        print(f"Failed to find course for evaluation {evaluation}")
        return

    # Enrollment statistics and extras
    statistics, _ = database.get_or_create(
        session, database.EvaluationStatistics, course=course,
    )
    statistics.enrolled = evaluation["enrollment"].get("enrolled", None)
    statistics.responses = evaluation["enrollment"].get("responses", None)
    statistics.declined = evaluation["enrollment"].get("declined", None)
    statistics.no_response = evaluation["enrollment"].get("no response", None)

    database.update_json(statistics, "extras", evaluation["extras"])

    # Resolve questions.
    def resolve_question(question_code, text, is_narrative, options=None):
        question, created = database.get_or_create(
            session, database.EvaluationQuestion, question_code=question_code,
        )
        if created:
            question.question_text = text
            question.is_narrative = is_narrative
            question.options = options
        else:
            # Sanity checks. Allows for variation in question text since sometimes the question
            # text includes the course code or title.
            if (
                question.is_narrative != is_narrative
                or textdistance.levenshtein.distance(question.question_text, text) > 32
                or not database.eq_json(question.options, options)
            ):
                raise database.InvariantError("Question codes are not consistent")
        return question

    # Evaluation ratings.
    for rating_info in evaluation["ratings"]:
        question = resolve_question(
            rating_info["question_id"],
            rating_info["question_text"],
            is_narrative=False,
            options=rating_info["options"],
        )

        # Update ratings data.
        rating, _ = database.get_or_create(
            session, database.EvaluationRating, course=course, question=question
        )
        rating.rating = rating_info["data"]
    # TODO remove extra entries

    # Evaluation narratives.
    for narrative_info in evaluation["narratives"]:
        question = resolve_question(
            narrative_info["question_id"],
            narrative_info["question_text"],
            is_narrative=True,
        )

        # Update narratives using comment list.
        comments = list(narrative_info["comments"])
        narratives = (
            session.query(database.EvaluationNarrative)
            .filter_by(course=course, question=question)
            .all()
        )
        for narrative in narratives:
            text = narrative.comment
            if text in comments:
                comments.remove(text)
            else:
                session.delete(narrative)
        for text in comments:
            narrative = database.EvaluationNarrative(
                course=course, question=question, comment=text
            )
            session.add(narrative)
    # TODO remove extra entries not associated with a listed question

    if session.new or session.deleted:
        pass
        # breakpoint()


if __name__ == "__main__":
    # allow the user to specify seasons (useful for testing and debugging)
    parser = argparse.ArgumentParser(description="Import classes")
    parser.add_argument(
        "-s",
        "--seasons",
        nargs="+",
        help="seasons to import (if empty, import all migrated+parsed courses)",
        default=None,
        required=False,
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=["courses", "evals", "demand", "all"],
        help="information to import: courses, evals, demand, or all (default)",
        default="all",
        required=False,
    )

    args = parser.parse_args()
    seasons = args.seasons

    # Course information.
    if seasons is None:

        # get full list of course seasons from files
        course_seasons = sorted(
            [
                filename.split(".")[0]  # remove the .json extension
                for filename in set(
                    os.listdir(f"{config.DATA_DIR}/migrated_courses/")
                    + os.listdir(f"{config.DATA_DIR}/parsed_courses/")
                )
                if filename[0] != "."
            ]
        )

        # get full list of demand seasons from files
        demand_seasons = sorted(
            [
                filename.split("_")[0]  # remove the _demand.json suffix
                for filename in os.listdir(f"{config.DATA_DIR}/demand_stats/")
                if filename[0] != "." and filename != "subjects.json"
            ]
        )
    else:
        course_seasons = seasons
        demand_seasons = seasons

    # Course listings.
    if args.mode == "courses" or args.mode == "all":
        print(f"Importing courses for season(s): {course_seasons}")
        for season in course_seasons:
            # Read the course listings, giving preference to freshly parsed over migrated ones.
            parsed_courses_file = Path(
                f"{config.DATA_DIR}/parsed_courses/{season}.json"
            )

            if parsed_courses_file.is_file():
                with open(parsed_courses_file, "r") as f:
                    parsed_course_info = ujson.load(f)
            else:
                # check migrated courses as a fallback
                migrated_courses_file = Path(
                    f"{config.DATA_DIR}/migrated_courses/{season}.json"
                )

                if not migrated_courses_file.is_file():
                    print(
                        f"Skipping season {season}: not found in parsed or migrated courses."
                    )
                    continue
                with open(migrated_courses_file, "r") as f:
                    parsed_course_info = ujson.load(f)

            for course_info in tqdm(
                parsed_course_info, desc=f"Importing courses in season {season}"
            ):
                with database.session_scope(database.Session) as session:
                    # tqdm.write(f"Importing {course_info}")
                    import_course(session, course_info)

    # Course demand.
    if args.mode == "demand" or args.mode == "all":
        # Compute seasons.

        print(f"Importing demand stats for seasons: {demand_seasons}")
        for season in demand_seasons:

            demand_file = Path(f"{config.DATA_DIR}/demand_stats/{season}_demand.json")

            if not demand_file.is_file():
                print(f"Skipping season {season}: demand statistics file not found.")
                continue

            with open(demand_file, "r") as f:
                demand_stats = ujson.load(f)

            for demand_info in tqdm(
                demand_stats, desc=f"Importing demand stats for {season}"
            ):
                with database.session_scope(database.Session) as session:
                    import_demand(session, season, demand_info)

    # Course evaluations.
    if args.mode == "evals" or args.mode == "all":
        all_evals = [
            filename
            for filename in set(
                os.listdir(f"{config.DATA_DIR}/previous_evals/")
                + os.listdir(f"{config.DATA_DIR}/course_evals/")
            )
            if filename[0] != "."
        ]

        # Filter by seasons.
        if seasons is None:
            evals_to_import = sorted(list(all_evals))

        else:
            evals_to_import = sorted(
                filename for filename in all_evals if filename.split("-")[0] in seasons
            )

        for filename in tqdm(evals_to_import, desc="Importing evaluations"):
            # Read the evaluation, giving preference to current over previous.
            current_evals_file = Path(f"{config.DATA_DIR}/course_evals/{filename}")

            if current_evals_file.is_file():
                with open(current_evals_file, "r") as f:
                    evaluation = ujson.load(f)
            else:
                with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
                    evaluation = ujson.load(f)

            with database.session_scope(database.Session) as session:
                # tqdm.write(f"Importing evaluation {evaluation}")
                import_evaluation(session, evaluation)

from tqdm import tqdm
import ujson
import os
import textdistance

import argparse

from ferry import config
from ferry import database

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
    database.update_json(statistics, "enrollment", evaluation["enrollment"])
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
        help="seasons to import",
        default=None,
        required=False,
    )
    parser.add_argument(
        "--mode",
        choices=["evals", "courses", "both"],
        help="import courses only",
        default="both",
        required=False,
    )

    args = parser.parse_args()

    seasons = args.seasons
    if seasons is None:
        # get the list of all course JSON files as previously fetched
        with open(f"{config.DATA_DIR}/seasons.json", "r") as f:
            seasons = ujson.load(f)

    # Course information.
    if args.mode != "evals":
        for season in seasons:
            with open(f"{config.DATA_DIR}/parsed_courses/{season}.json", "r") as f:
                parsed_course_info = ujson.load(f)

            for course_info in tqdm(
                parsed_course_info, desc=f"Importing courses in season {season}"
            ):
                with database.session_scope(database.Session) as session:
                    # tqdm.write(f"Importing {course_info}")
                    import_course(session, course_info)

    # Course evaluations.
    if args.mode != "courses":
        all_evals = set(
            os.listdir(f"{config.DATA_DIR}/previous_evals/")
            + os.listdir(f"{config.DATA_DIR}/course_evals/")
        )

        evals_to_import = sorted(
            filename for filename in all_evals if filename.split("-")[0] in seasons
        )

        for filename in tqdm(evals_to_import, desc="Importing evaluations"):
            # Read the evaluation, giving preference to current over previous.
            if os.path.isfile(f"{config.DATA_DIR}/course_evals/{filename}"):
                with open(f"{config.DATA_DIR}/course_evals/{filename}", "r") as f:
                    evaluation = ujson.load(f)
            else:
                with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
                    evaluation = ujson.load(f)

            with database.session_scope(database.Session) as session:
                # tqdm.write(f"Importing evaluation {evaluation}")
                import_evaluation(session, evaluation)

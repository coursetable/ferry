import collections
import csv
from typing import List

import sqlalchemy
from tqdm import tqdm

from ferry import config, database


"""
This script recomputes and sets all computed fields in the database.
It also checks the database invariants in the process.
"""


def seasons_computed(session):
    """
    Compute fields: season.year and season.term
    """
    for season in session.query(database.Season):  # type: database.Season
        season_code = season.season_code
        if len(season_code) != 6:
            raise database.InvariantError(
                "season code is not formatted correctly")
        season.year = int(season_code[:4])
        season.term = {"1": "spring", "2": "summer",
                       "3": "fall"}[season_code[-1]]


def listings_computed(session):
    """
    Compute field: listing.course_code
    """
    for listing in session.query(database.Listing):  # type: database.Listing
        listing.course_code = f"{listing.subject} {listing.number}"


def listing_invariants(session):
    """
    Check invariant: listing.season_code == course.season_code if listing.course_id == course.course_id.
    """
    for listing_id, course_id, listing_season_code, course_season_code in session.query(
        database.Listing.listing_id,
        database.Listing.course_id,
        database.Listing.season_code,
        database.Course.season_code,
    ).filter(database.Listing.course_id == database.Course.course_id):
        if listing_season_code != course_season_code:
            raise database.InvariantError(
                f"listing {listing_id} has mismatched season_code with course {course_id}"
            )


def question_invariants(session):
    """
    Check invariant: evaluation_questions.options is null iff evaluation_questions.is_narrative = True
    """
    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        narrative = question.is_narrative
        options = bool(question.options)
        if narrative and options:
            raise database.InvariantError(f"narrative question {question} has options")
        if not narrative and not options:
            raise database.InvariantError(f"ratings question {question} lacks options")


def questions_computed(session):
    """
    Populate field: evaluation_question.tag
    """

    # Build tag lookup table from file.
    tags = collections.defaultdict(lambda: None)
    with open(f"{config.RESOURCE_DIR}/question_tags.csv") as f:
        for question_code, tag in csv.reader(f):
            if tag:
                tags[question_code] = tag

    # Set tags on questions.
    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        tag = tags[question.question_code]
        question.tag = tag


def question_tag_invariant(session):
    """
    Check invariant: all questions sharing a tag also share is_narrative and len(options)
    """
    # Dictionary of question_code -> (is_narrative, len(options))
    tag_cache = {}

    def optlen(l): return len(l) if l else -1

    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        if not question.tag:
            continue

        if question.tag not in tag_cache:
            tag_cache[question.tag] = (
                question.is_narrative, optlen(question.options))
        else:
            narrative, count = tag_cache[question.tag]
            if question.is_narrative != narrative or count != optlen(question.options):
                raise database.InvariantError(f"mismatched tag {question.tag}")


def evaluation_statistics_computed(session):
    """
    Compute fields: evaluation_statistics.{avg_rating, avg_workload}
    """

    def fetch_ratings(course_id, question_tag) -> List[int]:
        ratings = (
            session.query(database.EvaluationRating)
            .filter(
                database.EvaluationRating.question_code
                == database.EvaluationQuestion.question_code
            )
            .filter(database.EvaluationRating.course_id == course_id)
            .filter(database.EvaluationQuestion.tag == question_tag)
        ).all()

        if not ratings:
            return None

        # Aggregate responses across question variants.
        data = [entry.rating for entry in ratings]
        rating = [sum(x) for x in zip(*data)]
        return rating

    def average_rating(ratings: List[int]) -> float:
        if not ratings or not sum(ratings):
            return None
        agg = 0
        for i, rating in enumerate(ratings):
            multiplier = i + 1
            agg += multiplier * rating
        return agg / sum(ratings)

    for evaluation_statistics in tqdm(
        session.query(database.EvaluationStatistics).all()
    ):  # type: database.EvaluationStatistics
        overall_ratings = fetch_ratings(
            evaluation_statistics.course_id, "rating")
        workload_ratings = fetch_ratings(
            evaluation_statistics.course_id, "workload")

        evaluation_statistics.avg_rating = average_rating(overall_ratings)
        evaluation_statistics.avg_workload = average_rating(workload_ratings)


def course_invariants(session):
    """
    Invariant: every course should have at least one listing.
    """
    courses_no_listings = (
        session.query(database.Course)
        .select_from(database.Listing)
        .join(database.Listing.course, isouter=True)
        .group_by(database.Course.course_id)
        .having(sqlalchemy.func.count(database.Listing.listing_id) == 0)
    ).all()

    if courses_no_listings:
        raise database.InvariantError(
            f"the following courses have no listings: {', '.join(str(course) for course in courses_no_listings)}"
        )


def historial_ratings_computed(session):
    """
    Update: historical_ratings (create entries as needed)
    """

    query = session.query(database.Course)

    for course in tqdm(query.all()):

        course_id = course.course_id

        for professor in course.professors:
            historical_ratings, _ = database.get_or_create(
                session,
                database.HistoricalRating,
                course_id=course_id,
                professor_id=professor.professor_id,
            )

            # Course rating - all professors.
            rating_all = (
                session.query(
                    sqlalchemy.func.avg(
                        database.EvaluationStatistics.avg_rating)
                )
                .select_from(database.Listing)
                .join(database.Course)
                .join(database.EvaluationStatistics)
                .filter(database.Listing.course_id == course_id)
            )
            historical_ratings.course_rating_all_profs = rating_all.scalar()

            # Course rating - this professor.
            rating_this = (
                session.query(
                    sqlalchemy.func.avg(
                        database.EvaluationStatistics.avg_rating)
                )
                .select_from(database.Listing)
                .join(database.Course)
                .join(database.EvaluationStatistics)
                .join(database.course_professors)
                .filter(database.Listing.course_id == course_id)
                .filter(
                    database.course_professors.c.professor_id == professor.professor_id
                )
            )
            historical_ratings.course_rating_this_prof = rating_this.scalar()

            # Course workload.
            workload_all = (
                session.query(
                    sqlalchemy.func.avg(
                        database.EvaluationStatistics.avg_workload)
                )
                .select_from(database.Listing)
                .join(database.Course)
                .join(database.EvaluationStatistics)
                .filter(database.Listing.course_id == course_id)
            )
            historical_ratings.course_workload = workload_all.scalar()


def professors_computed(session):
    """
    Compute field: professor.average_rating
    """
    query = (
        session.query(
            database.Professor,
            sqlalchemy.func.avg(database.EvaluationStatistics.avg_rating),
        )
        .select_from(database.course_professors)
        .join(database.Course)
        .join(database.EvaluationStatistics)
        .join(database.Professor)
        .group_by(database.Professor.professor_id)
    )

    for professor, average_rating in query:
        professor.average_rating = average_rating


if __name__ == "__main__":
    items = [
        # seasons_computed,
        # listing_invariants,
        # course_invariants,
        # listings_computed,
        # question_invariants,
        # questions_computed,
        # question_tag_invariant,
        # evaluation_statistics_computed,
        historial_ratings_computed,
        professors_computed,
    ]

    for fn in items:
        if fn.__doc__:
            tqdm.write(f"{fn.__doc__.strip()}")
        else:
            tqdm.write(f"Running: {fn.__name__}")

        with database.session_scope(database.Session) as session:
            fn(session)

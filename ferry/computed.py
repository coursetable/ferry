from tqdm import tqdm
import csv
import collections
import database

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
            raise database.InvariantError("season code is not formatted correctly")
        season.year = int(season_code[:4])
        season.term = {"1": "spring", "2": "summer", "3": "fall"}[season_code[-1]]


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
    with open("./question_tags.csv") as f:
        for question_code, tag in csv.reader(f):
            if tag:
                tags[question_code] = tag

    # Set tags on questions.
    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        question.tag = tags[question.question_code]


def question_tag_invariant(session):
    """
    Check invariant: all questions sharing a tag also share is_narrative and len(options)
    """
    # Dictionary of question_code -> (is_narrative, len(options))
    tag_cache = {}
    optlen = lambda l: len(l) if l else -1

    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        if not question.tag:
            continue

        if question.tag not in tag_cache:
            tag_cache[question.tag] = (question.is_narrative, optlen(question.options))
        else:
            narrative, count = tag_cache[question.tag]
            if question.is_narrative != narrative or count != optlen(question.options):
                raise database.InvariantError(f"mismatched tag {question.tag}")


if __name__ == "__main__":
    items = [
        seasons_computed,
        listing_invariants,
        listings_computed,
        question_invariants,
        questions_computed,
        question_tag_invariant,
    ]

    for fn in items:
        if fn.__doc__:
            tqdm.write(f"{fn.__doc__.strip()}")
        else:
            tqdm.write(f"Running: {fn.__name__}")

        with database.session_scope(database.Session) as session:
            fn(session)

"""
Invariant: every course should have at least one listing.
"""

"""
   81:         comment="[computed] Student enrollment (retrieved from evaluations, not part of the Courses API)",
   86:         comment="[computed] Whether or not a different professor taught the class when it was this size",
  119:         comment="[computed] Average overall course rating (from this course's evaluations, aggregated across cross-listings)",
  123:         comment="[computed] Average workload rating ((from this course's evaluations, aggregated across cross-listings)",
  196:         comment='[computed] Average rating of the professor assessed via the "Overall assessment" question in courses taught',
  214:         comment="[computed] The average rating for this course code, across all professors who taught it",
  218:         comment="[computed] The average rating for this course code when taught by this professor",
  222:         comment="[computed] The average workload for this course code, across all times it was taught",
  299:         comment="[computed] The length of the comment response",
"""

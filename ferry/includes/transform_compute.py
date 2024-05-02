"""
Handles computed fields in the tables.

Used by /ferry/transform.py.
"""

import csv
import math

import pandas as pd
from tqdm import tqdm
import logging

from ferry import database
from ferry.includes.same_courses import (
    resolve_historical_courses,
    split_same_professors,
)
from ferry.includes.utils import get_table_columns

QUESTION_TAGS = {}

from pathlib import Path

resource_dir = Path(__file__).parent.parent / "resources"

with open(f"{resource_dir}/question_tags.csv") as f:
    for question_code, tag in csv.reader(f):
        QUESTION_TAGS[question_code] = tag


def questions_computed(evaluation_questions: pd.DataFrame) -> pd.DataFrame:
    """
    Populate computed question fields:

    Parameters
    ----------
    evaluation_questions:
        Pandas tables post-import.

    Returns
    -------
    evaluation_questions:
        table with computed fields.
    """

    logging.debug("Assigning question tags")

    def assign_code(row):

        code = row["question_code"]

        # Remove these suffixes for tag resolution.
        strip_suffixes = ["-YCWR", "-YXWR", "-SA"]

        for suffix in strip_suffixes:
            if code.endswith(suffix):
                code = code[: -len(suffix)]
                break

        # Set the appropriate question tag.
        try:
            return QUESTION_TAGS[code]
        except KeyError as err:
            raise database.InvariantError(
                f"No associated tag for question code {code} with text {row['question_text']}"
            ) from err

    evaluation_questions["tag"] = evaluation_questions.apply(assign_code, axis=1)

    return evaluation_questions


def evaluation_statistics_computed(
    evaluation_statistics: pd.DataFrame,
    evaluation_ratings: pd.DataFrame,
    evaluation_questions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Populate computed question fields:
        avg_rating:
            Average course rating.
        avg_workload:
            Average course workload.

    Parameters
    ----------
    Pandas tables post-import:
        evaluation_statistics
        evaluation_ratings
        evaluation_questions

    Returns
    -------
    evaluation_statistics:
        Table with computed fields.
    """
    logging.debug("Computing average ratings by course")

    # create local deep copy
    evaluation_ratings = evaluation_ratings.copy(deep=True)

    # match tags to ratings
    question_code_map = dict(
        zip(evaluation_questions["question_code"], evaluation_questions["tag"])
    )
    evaluation_ratings["tag"] = evaluation_ratings["question_code"].apply(
        question_code_map.get
    )

    # compute average rating of responses array
    def average_rating(ratings: list[int]) -> float | None:
        if not ratings or not sum(ratings):
            return None
        agg = 0
        for i, rating in enumerate(ratings):
            multiplier = i + 1
            agg += multiplier * rating
        return agg / sum(ratings)

    # Get average rating for each course with a specified tag
    def average_by_course(question_tag, n_categories):

        tagged_ratings = evaluation_ratings[
            evaluation_ratings["tag"] == question_tag
        ].copy(deep=True)
        rating_by_course = tagged_ratings.groupby("course_id")["rating"].apply(list)

        # Aggregate responses across question variants.
        rating_by_course = rating_by_course.apply(
            lambda data: [sum(x) for x in zip(*data)]
        )

        # check that all the response arrays are the expected length
        lengths_invalid = rating_by_course.apply(len) != n_categories

        if any(lengths_invalid):
            raise database.InvariantError(
                f"""
                Invalid workload responses\n
                \tExpected length of 5: {rating_by_course[lengths_invalid]}
                """
            )

        rating_by_course = rating_by_course.apply(average_rating)
        rating_by_course = rating_by_course.to_dict()

        return rating_by_course

    # get overall and workload ratings
    overall_by_course = average_by_course("rating", 5)
    workload_by_course = average_by_course("workload", 5)

    evaluation_statistics["avg_rating"] = evaluation_statistics["course_id"].apply(
        overall_by_course.get
    )
    evaluation_statistics["avg_workload"] = evaluation_statistics["course_id"].apply(
        workload_by_course.get
    )

    return evaluation_statistics


def courses_computed(
    courses: pd.DataFrame,
    listings: pd.DataFrame,
    evaluation_statistics: pd.DataFrame,
    course_professors: pd.DataFrame,
) -> pd.DataFrame:
    """
    Populates computed course rating fields:
        average_rating:
            Average course rating over all past instances.
        average_workload:
            Average course workload over all past instances.

    Also populates last-offered course fields:
        last_offered_course_id:
            course_id of the most recent previous offering.
        last_enrollment_course_id:
            course_id of the most recent previous offering with enrollment statistics.
        last_enrollment:
            Number of students in most recent previous offering with enrollment statistics.
        last_enrollment_season_code:
            Season of recent previous offering with enrollment statistics.
        last_enrollment_same_professors:
            If recent previous offering with enrollment statistics was with same professors.

    Parameters
    ----------
    Pandas tables post-import:
        courses
        listings
        evaluation_statistics
        course_professors

    Returns
    -------
    courses:
        Table with computed fields.
    """
    logging.debug("Computing courses")

    listings = listings.copy(deep=True)
    evaluation_statistics = evaluation_statistics.copy(deep=True)
    course_professors = course_professors.copy(deep=True)

    (
        course_to_same_course,
        same_course_to_courses,
        course_to_same_course_filtered,
        same_course_to_courses_filtered,
    ) = resolve_historical_courses(courses, listings)

    # partition ID of same-codes courses (not used anymore, useful for debugging)
    courses["shared_code_id"] = courses["course_id"].apply(course_to_same_course.get)
    # connected courses with the same code (not used anymore, useful for debugging)
    courses["shared_code_courses"] = courses["shared_code_id"].apply(
        same_course_to_courses.get
    )

    # unique ID for each partition of the same courses
    courses["same_course_id"] = courses["course_id"].apply(
        course_to_same_course_filtered.get
    )

    # list of course_ids that are the same course per course_id
    courses["same_courses"] = courses["same_course_id"].apply(
        same_course_to_courses_filtered.get
    )

    # split same-course partition by same-professors
    course_to_same_prof_course, same_prof_course_to_courses = split_same_professors(
        course_to_same_course_filtered, course_professors
    )

    # unique ID for each partition of the same courses taught by the same set of profs
    courses["same_course_and_profs_id"] = courses["course_id"].apply(
        course_to_same_prof_course.get
    )

    # list of course_ids that are the same course and taught by same profs per course_id
    courses["same_courses_and_profs"] = courses["same_course_and_profs_id"].apply(
        same_prof_course_to_courses.get
    )

    # map course_id to professor_ids
    # use frozenset because it is hashable (set is not), needed for groupby
    course_to_professors = course_professors.groupby("course_id")["professor_id"].apply(
        frozenset
    )

    # get historical offerings with same professors
    listings["professors"] = listings["course_id"].apply(course_to_professors.get)
    courses["professors"] = courses["course_id"].apply(course_to_professors.get)

    logging.debug("Computing last offering statistics")

    # course_id for all evaluated courses
    evaluated_courses = set(
        evaluation_statistics.dropna(subset=["enrolled"], axis=0)["course_id"]
    )

    # map course_id to season
    course_to_season = dict(zip(courses["course_id"], courses["season_code"]))

    # map course_id to number enrolled
    course_to_enrollment = dict(
        zip(
            evaluation_statistics["course_id"],
            evaluation_statistics["enrolled"],
        )
    )

    # get last course offering in general (with or without enrollment)
    def get_last_offered(course_row):
        same_courses = course_row["same_courses"]

        same_courses = [
            x for x in same_courses if course_to_season[x] < course_row["season_code"]
        ]

        if len(same_courses) == 0:
            return None

        same_courses = [x for x in same_courses if x is not course_row["course_id"]]
        if len(same_courses) == 0:
            return None

        last_offered_course = max(same_courses, key=lambda x: course_to_season[x])

        return last_offered_course

    # helper function for getting enrollment fields of last-offered course
    def get_last_offered_enrollment(course_row):
        same_courses = course_row["same_courses"]

        # keep course only if distinct, has enrollment statistics, and is before current
        same_courses = [
            x
            for x in same_courses
            if x in evaluated_courses
            and course_to_season[x] < course_row["season_code"]
        ]
        if len(same_courses) == 0:
            return [None, None, None, None]
        same_courses = [x for x in same_courses if x is not course_row["course_id"]]
        if len(same_courses) == 0:
            return [None, None, None, None]

        current_professors = course_to_professors.get(course_row["course_id"], set())

        # sort courses newest-first
        same_courses = sorted(
            same_courses, key=lambda x: course_to_season[x], reverse=True
        )

        # get the newest course with the same professors, otherwise just the newest course
        last_enrollment_course = next(
            (
                prev_course
                for prev_course in same_courses
                if course_to_professors.get(prev_course, set()) == current_professors
            ),
            # default to newest course if no previous course has same profs
            same_courses[0],
        )

        # number of students last taking course
        last_enrollment = course_to_enrollment[last_enrollment_course]
        # season for last enrollment
        last_enrollment_season = course_to_season[last_enrollment_course]
        # professors for last enrollment
        last_enrollment_professors = course_to_professors.get(
            last_enrollment_course, set()
        )

        # if last enrollment is with same professors
        last_enrollment_same_professors = (
            last_enrollment_professors == current_professors
        )

        return (
            last_enrollment_course,
            last_enrollment,
            last_enrollment_season,
            last_enrollment_same_professors,
        )

    tqdm.pandas(desc="Finding last-offered course", leave=False)
    courses["last_offered_course_id"] = courses.progress_apply(get_last_offered, axis=1)

    tqdm.pandas(desc="Finding last-offered enrollment", leave=False)
    # getting last-offered enrollment
    (
        courses["last_enrollment_course_id"],
        courses["last_enrollment"],
        courses["last_enrollment_season_code"],
        courses["last_enrollment_same_professors"],
    ) = zip(*courses.progress_apply(get_last_offered_enrollment, axis=1))

    logging.debug("Computing historical ratings for courses")

    # map courses to ratings
    course_to_overall = dict(
        zip(
            evaluation_statistics["course_id"],
            evaluation_statistics["avg_rating"],
        )
    )
    course_to_workload = dict(
        zip(
            evaluation_statistics["course_id"],
            evaluation_statistics["avg_workload"],
        )
    )

    # get ratings
    courses["average_rating"] = courses["same_courses"].apply(
        lambda courses: [course_to_overall.get(x) for x in courses]
    )
    courses["average_workload"] = courses["same_courses"].apply(
        lambda courses: [course_to_workload.get(x) for x in courses]
    )

    courses["average_rating_same_professors"] = courses["same_courses_and_profs"].apply(
        lambda courses: [course_to_overall.get(x) for x in courses]
    )
    courses["average_workload_same_professors"] = courses[
        "same_courses_and_profs"
    ].apply(lambda courses: [course_to_workload.get(x) for x in courses])

    # calculate the average of an array
    def average(nums):
        nums = list(filter(lambda x: x is not None, nums))
        nums = list(filter(lambda x: not math.isnan(x), nums))
        if not nums:
            return [None, None]
        num_obs = len(nums)
        return (sum(nums) / num_obs, num_obs)

    # calculate averages over past offerings
    for average_col, num_col in [
        ("average_rating", "average_rating_n"),
        ("average_workload", "average_workload_n"),
        ("average_rating_same_professors", "average_rating_same_professors_n"),
        (
            "average_workload_same_professors",
            "average_workload_same_professors_n",
        ),
    ]:
        courses[average_col], courses[num_col] = zip(
            *courses[average_col].apply(average)
        )

    # remove intermediate columns
    courses = courses.loc[:, get_table_columns(database.models.Course)]

    return courses


def professors_computed(
    professors: pd.DataFrame,
    course_professors: pd.DataFrame,
    evaluation_statistics: pd.DataFrame,
) -> pd.DataFrame:
    """
    Populate computed professor fields:
        average_rating:
            Average overall rating of classes taught.

    Parameters
    ----------
    Pandas tables post-import:
        professors
        course_professors
        evaluation_statistics

    Returns
    -------
    professors:
        Table with computed fields.
    """
    logging.debug("Computing ratings for professors")

    # create local deep copy
    course_professors = course_professors.copy(deep=True)

    course_to_overall = dict(
        zip(
            evaluation_statistics["course_id"],
            evaluation_statistics["avg_rating"],
        )
    )
    # course_to_workload = dict(
    #     zip(evaluation_statistics["course_id"], evaluation_statistics["avg_workload"])
    # )

    course_professors["average_rating"] = course_professors["course_id"].apply(
        course_to_overall.get
    )
    # course_professors["average_workload"] = course_professors["course_id"].apply(
    #     course_to_workload.get
    # )

    rating_by_professor = (
        course_professors.dropna(subset=["average_rating"])
        .groupby("professor_id")["average_rating"]
        .mean()
        .to_dict()
    )

    rating_by_professor_n = (
        course_professors.dropna(subset=["average_rating"])
        .groupby("professor_id")["average_rating"]
        .count()
        .to_dict()
    )
    # workload_by_professor = (
    #     course_professors.dropna("average_workload")
    #     .groupby("professor_id")
    #     .mean()
    #     .to_dict()
    # )

    professors["average_rating"] = professors["professor_id"].apply(
        rating_by_professor.get
    )

    professors["average_rating_n"] = professors["professor_id"].apply(
        rating_by_professor_n.get
    )

    return professors

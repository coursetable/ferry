import csv
from typing import List

import pandas as pd
import ujson

from ferry import config, database
from ferry.includes.tqdm import tqdm
from ferry.includes.utils import get_table_columns

tqdm.pandas()

QUESTION_TAGS = dict()
with open(f"{config.RESOURCE_DIR}/question_tags.csv") as f:
    for question_code, tag in csv.reader(f):
        QUESTION_TAGS[question_code] = tag


def questions_computed(evaluation_questions):
    """
    Populate computed question fields:
        tags: question tags for ratings aggregation

    Parameters
    ----------
    Pandas tables post-import:
        evaluation_questions

    Returns
    -------
    evaluation_questions: table with computed fields

    """

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
        except KeyError as e:
            raise database.InvariantError(
                f"No associated tag for question code {code} with text {row['question_text']}"
            )

    evaluation_questions["tag"] = evaluation_questions.apply(assign_code, axis=1)

    return evaluation_questions


def evaluation_statistics_computed(
    evaluation_statistics, evaluation_ratings, evaluation_questions
):
    """
    Populate computed question fields:
        avg_rating: average course rating
        avg_workload: average course workload

    Parameters
    ----------
    Pandas tables post-import:
        evaluation_statistics
        evaluation_ratings
        evaluation_questions

    Returns
    -------
    evaluation_statistics: table with computed fields

    """

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
    def average_rating(ratings: List[int]) -> float:
        if not ratings or not sum(ratings):
            return None
        agg = 0
        for i, rating in enumerate(ratings):
            multiplier = i + 1
            agg += multiplier * rating
        return agg / sum(ratings)

    # Get average rating for each course with a specified tag
    def average_by_course(tag, n_categories):

        tagged_ratings = evaluation_ratings[evaluation_ratings["tag"] == tag].copy(
            deep=True
        )
        rating_by_course = tagged_ratings.groupby("course_id")["rating"].apply(list)

        # Aggregate responses across question variants.
        rating_by_course = rating_by_course.apply(
            lambda data: [sum(x) for x in zip(*data)]
        )

        # check that all the response arrays are the expected length
        lengths_invalid = rating_by_course.apply(len) != n_categories

        if any(lengths_invalid):
            raise database.InvariantError(
                f"Invalid workload responses, expected length of 5: {rating_by_course[lengths_invalid]}"
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


def courses_computed(courses, listings, evaluation_statistics, course_professors):
    """
    Populate computed course fields:
        average_rating: average course rating over all past instances
        average_workload: average course workload over all past instances

    Parameters
    ----------
    Pandas tables post-import:
        courses
        listings
        evaluation_statistics

    Returns
    -------
    courses: table with computed fields

    """

    # create local deep copy
    courses = courses.copy(deep=True)

    # map courses to codes and codes to courses for historical offerings
    course_to_codes = listings.groupby("course_id")["course_code"].apply(list).to_dict()
    code_to_courses = listings.groupby("course_code")["course_id"].apply(list).to_dict()
    courses["codes"] = courses["course_id"].apply(course_to_codes.get)
    courses["coded_courses"] = courses["codes"].apply(
        lambda x: [code_to_courses[code] for code in x]
    )

    # transform a list of lists into a single list
    def flatten_list_of_lists(list_of_lists):

        flattened = [x for y in list_of_lists for x in y]

        return flattened

    courses["coded_courses"] = courses["coded_courses"].apply(flatten_list_of_lists)
    courses["coded_courses"] = courses["coded_courses"].apply(lambda x: list(set(x)))

    print("Computing last offering statistics")

    evaluated_courses = set(
        evaluation_statistics.dropna(subset=["enrolled"], axis=0)["course_id"]
    )

    course_to_season = dict(zip(courses["course_id"], courses["season_code"]))

    course_to_enrollment = dict(
        zip(evaluation_statistics["course_id"], evaluation_statistics["enrolled"])
    )
    course_to_professors = course_professors.groupby("course_id")["professor_id"].apply(
        set
    )

    def get_last_offered_enrollment(course_row):
        coded_courses = course_row["coded_courses"]

        # keep course only if distinct, has enrollment statistics, and is before current
        coded_courses = [
            x
            for x in coded_courses
            if x in evaluated_courses
            and course_to_season[x] < course_row["season_code"]
        ]
        if len(coded_courses) == 0:
            return [None, None, None, None]
        coded_courses = [x for x in coded_courses if x is not course_row["course_id"]]
        if len(coded_courses) == 0:
            return [None, None, None, None]

        # extract the latest offering
        last_enrollment_course = max(coded_courses, key=lambda x: course_to_season[x])

        # number of students last taking course
        last_enrollment = course_to_enrollment[last_enrollment_course]
        # season for last enrollment
        last_enrollment_season = course_to_season[last_enrollment_course]
        # professors for last enrollment
        last_enrollment_professors = course_to_professors.get(
            last_enrollment_course, set()
        )

        current_professors = course_to_professors.get(course_row["course_id"], set())

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

    # getting last-offered enrollment
    (
        courses["last_enrollment_course_id"],
        courses["last_enrollment"],
        courses["last_enrollment_season_code"],
        courses["last_enrollment_same_professors"],
    ) = zip(*courses.progress_apply(get_last_offered_enrollment, axis=1))

    print("Computing historical ratings for courses")

    # calculate the average of an array
    def average(nums):
        nums = list(filter(lambda x: x is not None, nums))
        nums = list(filter(lambda x: x == x, nums))
        if not nums:
            return None
        return sum(nums) / len(nums)

    # map courses to ratings
    course_to_overall = dict(
        zip(evaluation_statistics["course_id"], evaluation_statistics["avg_rating"])
    )
    course_to_workload = dict(
        zip(evaluation_statistics["course_id"], evaluation_statistics["avg_workload"])
    )

    # get ratings
    courses["average_rating"] = courses["coded_courses"].apply(
        lambda courses: [course_to_overall.get(x, None) for x in courses]
    )
    courses["average_workload"] = courses["coded_courses"].apply(
        lambda courses: [course_to_workload.get(x, None) for x in courses]
    )

    # calculate averages over past offerings
    courses["average_rating"] = courses["average_rating"].apply(average)
    courses["average_workload"] = courses["average_workload"].apply(average)

    # remove intermediate columns
    courses = courses.loc[:, get_table_columns(database.models.Course)]

    return courses


def professors_computed(professors, course_professors, evaluation_statistics):

    """
    Populate computed professor fields:
        average_rating: average overall rating of classes taught

    Parameters
    ----------
    Pandas tables post-import:
        professors
        course_professors
        evaluation_statistics

    Returns
    -------
    professors: table with computed fields

    """

    # create local deep copy
    course_professors = course_professors.copy(deep=True)

    course_to_overall = dict(
        zip(evaluation_statistics["course_id"], evaluation_statistics["avg_rating"])
    )
    # course_to_workload = dict(
    #     zip(evaluation_statistics["course_id"], evaluation_statistics["avg_workload"])
    # )

    course_professors["average_rating"] = course_professors["course_id"].apply(
        course_to_overall.get
    )
    # course_professors["average_workload"] = course_professors["course_id"].apply(course_to_workload.get)

    rating_by_professor = (
        course_professors.dropna(subset=["average_rating"])
        .groupby("professor_id")["average_rating"]
        .mean()
        .to_dict()
    )
    # workload_by_professor = course_professors.dropna("average_workload").groupby("professor_id").mean().to_dict()

    professors["average_rating"] = professors["professor_id"].apply(
        rating_by_professor.get
    )

    return professors

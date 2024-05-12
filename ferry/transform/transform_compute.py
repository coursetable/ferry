import math
import re
from typing import cast

import pandas as pd
from tqdm import tqdm
import logging
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ferry import database
from ferry.transform.same_courses import (
    resolve_historical_courses,
    split_same_professors,
)


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

    def assign_code(row: pd.Series):
        text = cast(str, row["question_text"]).lower()

        tag_candidates = {
            "Available resources": "resources" in text,
            "Engagement": "engagement" in text,
            "Feedback": "feedback" in text,
            "Intellectual challenge": "intellectual challenge" in text,
            "Major": "major" in text,
            "Organization": "organize" in text,
            "Professor": bool(re.search(r"rating|assessment|evaluate", text))
            and "instructor" in text,
            "Overall": "overall assessment" in text and "instructor" not in text
            # This one is used in rating average
            and not row["is_narrative"],
            "Recommend": "recommend" in text,
            # SU122: "How will you use the knowledge and skills you learned in
            # this course in your future endeavors?"
            # There is another SU question about skills; do not assign tag for
            # this one to avoid collision
            "Skills": "skills" in text and row["question_code"] not in ["SU122"],
            "Strengths/weaknesses": "strengths and weaknesses" in text
            and "instructor" not in text,
            "Summary": "summarize" in text and "recommend" not in text,
            # This one is used in rating average
            "Workload": "workload" in text and not row["is_narrative"],
        }

        if sum(tag_candidates.values()) > 1:
            raise database.InvariantError(
                f"{row['question_text']} contains multiple tags {', '.join([tag for tag, condition in tag_candidates.items() if condition])}. Please adjust the conditions above."
            )

        return next(
            (tag for tag, condition in tag_candidates.items() if condition), None
        )

    evaluation_questions["tag"] = evaluation_questions.apply(assign_code, axis=1)

    return evaluation_questions


analyzer = SentimentIntensityAnalyzer()


def sentiment_analysis(text: str) -> tuple[float, float, float, float]:
    sentiment = analyzer.polarity_scores(text)
    return sentiment["neg"], sentiment["neu"], sentiment["pos"], sentiment["compound"]


def narratives_computed(evaluation_narratives: pd.DataFrame) -> pd.DataFrame:
    (
        evaluation_narratives["comment_neg"],
        evaluation_narratives["comment_neu"],
        evaluation_narratives["comment_pos"],
        evaluation_narratives["comment_compound"],
    ) = zip(*evaluation_narratives["comment"].apply(sentiment_analysis))
    return evaluation_narratives


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
    overall_by_course = average_by_course("Overall", 5)
    workload_by_course = average_by_course("Workload", 5)

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
    professors: pd.DataFrame,
) -> pd.DataFrame:
    """
    Populates computed course rating fields:
        average_gut_rating
        average_professor_rating:
            Average of the average ratings of all professors for this course.
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

    Must be called after professors_computed because it uses the average_rating of each professor.
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
            return None, None, None, None
        same_courses = [x for x in same_courses if x is not course_row["course_id"]]
        if len(same_courses) == 0:
            return None, None, None, None

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
    courses["last_offered_course_id"] = courses["last_offered_course_id"].astype(
        pd.Int64Dtype()
    )

    tqdm.pandas(desc="Finding last-offered enrollment", leave=False)
    # getting last-offered enrollment
    (
        courses["last_enrollment_course_id"],
        courses["last_enrollment"],
        courses["last_enrollment_season_code"],
        courses["last_enrollment_same_professors"],
    ) = zip(*courses.progress_apply(get_last_offered_enrollment, axis=1))
    courses["last_enrollment_course_id"] = courses["last_enrollment_course_id"].astype(
        pd.Int64Dtype()
    )
    courses["last_enrollment"] = courses["last_enrollment"].astype(pd.Int64Dtype())

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
    def average(nums: list[float]) -> tuple[float | None, int]:
        nums = [x for x in nums if x is not None and not math.isnan(x)]
        if not nums:
            return None, 0
        num_obs = len(nums)
        return sum(nums) / num_obs, num_obs

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
        courses[num_col] = courses[num_col].astype(pd.Int64Dtype())

    courses["average_gut_rating"] = (
        courses["average_rating"] - courses["average_workload"]
    )

    # Calculate average_professor_rating
    merged_data = pd.merge(courses[["course_id"]], course_professors, on="course_id")
    merged_data = pd.merge(
        merged_data, professors[["professor_id", "average_rating"]], on="professor_id"
    )
    average_professor_ratings = (
        merged_data.groupby("course_id")["average_rating"]
        .mean()
        .reset_index()
        .rename(columns={"average_rating": "average_professor_rating"})
    )
    courses = pd.merge(courses, average_professor_ratings, on="course_id", how="left")

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
    professors["average_rating_n"] = professors["average_rating_n"].astype(
        pd.Int64Dtype()
    )
    merged_data = pd.merge(course_professors, professors, on="professor_id", how="left")
    courses_taught = (
        merged_data.groupby("professor_id").size().reset_index(name="courses_taught")
    )
    professors = pd.merge(professors, courses_taught, on="professor_id", how="left")
    professors["courses_taught"] = professors["courses_taught"].fillna(0).astype(int)

    return professors

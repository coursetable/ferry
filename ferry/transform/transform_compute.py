import math
import re
from typing import cast

import numpy as np
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
    Populate the following fields on evaluation_questions:

    - tag
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
            and "course" in text
            and "teaching assistant" not in text
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
    """
    Populate the following fields on evaluation_narratives:

    - comment_neg
    - comment_neu
    - comment_pos
    - comment_compound
    """
    logging.debug("Computing comment sentiment")

    # (
    #     evaluation_narratives["comment_neg"],
    #     evaluation_narratives["comment_neu"],
    #     evaluation_narratives["comment_pos"],
    #     evaluation_narratives["comment_compound"],
    # ) = zip(*evaluation_narratives["comment"].apply(sentiment_analysis))

    # TODO: the sentiment analysis is by far the most costly, we need to cache it
    # We don't use this actually so it's fine for now
    evaluation_narratives["comment_neg"] = 0
    evaluation_narratives["comment_neu"] = 0
    evaluation_narratives["comment_pos"] = 0
    evaluation_narratives["comment_compound"] = 0
    return evaluation_narratives


def evaluation_statistics_computed(
    evaluation_statistics: pd.DataFrame,
    evaluation_ratings: pd.DataFrame,
    evaluation_questions: pd.DataFrame,
) -> pd.DataFrame:
    """
    Populate the following fields on evaluation_statistics:

    - avg_rating
    - avg_workload
    """
    logging.debug("Computing average ratings by course")

    # Match ratings with tag
    evaluation_ratings = pd.merge(
        evaluation_ratings, evaluation_questions, on="question_code", how="left"
    )

    # Get average rating for each course with a specified tag
    def average_by_course(question_tag: str, n_categories: int):
        tagged_ratings = evaluation_ratings[evaluation_ratings["tag"] == question_tag]
        # course_id -> Series[list[int]]
        rating_by_course = tagged_ratings.groupby("course_id")["rating"]

        def average_rating(data: pd.Series) -> float:
            # A course can have multiple questions of the same type. This usually
            # happens when the course is cross-listed between GS and YC
            weights = [sum(x) for x in zip(*data)]
            if len(weights) != n_categories:
                raise database.InvariantError(
                    f"Invalid number of categories for {question_tag}: {len(weights)}"
                )
            if sum(weights) == 0:
                return np.nan
            return cast(float, np.average(range(1, n_categories + 1), weights=weights))

        return rating_by_course.apply(average_rating)

    # get overall and workload ratings
    avg_rating = average_by_course("Overall", 5).reset_index(name="avg_rating")
    avg_workload = average_by_course("Workload", 5).reset_index(name="avg_workload")

    evaluation_statistics = pd.merge(
        evaluation_statistics, avg_rating, on="course_id", how="left"
    )
    evaluation_statistics = pd.merge(
        evaluation_statistics, avg_workload, on="course_id", how="left"
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
    """
    logging.debug("Computing courses")

    listings = listings.copy(deep=True)
    evaluation_statistics = evaluation_statistics.copy(deep=True)
    course_professors = course_professors.copy(deep=True)

    course_to_same_course, same_course_to_courses = resolve_historical_courses(
        courses, listings
    )

    # split same-course partition by same-professors
    course_to_same_prof_course, same_prof_course_to_courses = split_same_professors(
        course_to_same_course, course_professors
    )

    courses["same_course_id"] = courses["course_id"].apply(course_to_same_course.get)
    courses["same_course_and_profs_id"] = courses["course_id"].apply(
        course_to_same_prof_course.get
    )

    # map course_id to professor_ids
    # use frozenset because it is hashable (set is not), needed for groupby
    course_to_professors = course_professors.groupby("course_id")["professor_id"].apply(
        frozenset
    )

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
    def get_last_offered(course_row: pd.Series):
        same_courses = [
            x
            for x in same_course_to_courses[course_row["same_course_id"]]
            if course_to_season[x] < course_row["season_code"]
        ]

        if len(same_courses) == 0:
            return None

        same_courses = [x for x in same_courses if x is not course_row["course_id"]]
        if len(same_courses) == 0:
            return None

        last_offered_course = max(same_courses, key=lambda x: course_to_season[x])

        return last_offered_course

    # helper function for getting enrollment fields of last-offered course
    def get_last_offered_enrollment(course_row: pd.Series):
        # keep course only if distinct, has enrollment statistics, and is before current
        same_courses = [
            x
            for x in same_course_to_courses[course_row["same_course_id"]]
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
    courses["average_rating"] = courses["same_course_id"].apply(
        lambda id_: [course_to_overall.get(x) for x in same_course_to_courses[id_]]
    )
    courses["average_workload"] = courses["same_course_id"].apply(
        lambda id_: [course_to_workload.get(x) for x in same_course_to_courses[id_]]
    )

    courses["average_rating_same_professors"] = courses["same_course_and_profs_id"].apply(
        lambda id_: [course_to_overall.get(x) for x in same_prof_course_to_courses[id_]]
    )
    courses["average_workload_same_professors"] = courses[
        "same_course_and_profs_id"
    ].apply(lambda id_: [course_to_workload.get(x) for x in same_prof_course_to_courses[id_]])

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
    Populate the following fields on professors:

    - average_rating
    - average_rating_n
    - courses_taught
    """
    logging.debug("Computing ratings for professors")

    prof_to_ratings = (
        pd.merge(course_professors, evaluation_statistics, on="course_id", how="left")
        .groupby("professor_id")["avg_rating"]
        .apply(list)
        .reset_index(name="ratings")
    )

    def avg_prof_rating(row: pd.Series):
        ratings = list(filter(lambda x: not np.isnan(x), row["ratings"]))
        if ratings:
            # TODO: implement weights based on recency, class size, etc.
            mean = np.mean(ratings)
        else:
            mean = np.nan
        return {
            "professor_id": row["professor_id"],
            "average_rating": mean,
            "average_rating_n": len(ratings),
        }

    prof_to_ratings = prof_to_ratings.apply(
        avg_prof_rating, axis=1, result_type="expand"
    )

    professors = pd.merge(professors, prof_to_ratings, on="professor_id", how="left")
    professors["average_rating_n"] = professors["average_rating_n"].astype(
        pd.Int64Dtype()
    )

    courses_taught = (
        pd.merge(course_professors, professors, on="professor_id", how="left")
        .groupby("professor_id")
        .size()
        .reset_index(name="courses_taught")
    )
    professors = pd.merge(professors, courses_taught, on="professor_id", how="left")
    professors["courses_taught"] = professors["courses_taught"].fillna(0).astype(int)

    return professors

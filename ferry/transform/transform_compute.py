import math
import re
from typing import cast

import numpy as np
import pandas as pd
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
            # DR464: "Please provide feedback on the instructor's teaching style,
            # speaking and listening skills, and time management."
            "Feedback": "feedback" in text and row["question_code"] not in ["DR464"],
            "Intellectual challenge": "intellectual challenge" in text,
            "Major": "major" in text,
            "Organization": "organize" in text,
            # DR113, DR316: "I would recommend this instructor to other students."
            "Professor": bool(re.search(r"rating|assessment|evaluate", text))
            and "instructor" in text
            and row["question_code"] not in ["DR113", "DR316"],
            "Overall": "overall assessment" in text and "instructor" not in text
            # This one is used in rating average
            and not row["is_narrative"],
            # DR113, DR316: "I would recommend this instructor to other students."
            "Recommend": "recommend" in text
            and row["question_code"] not in ["DR113", "DR316"],
            # SU122: "How will you use the knowledge and skills you learned in
            # this course in your future endeavors?"
            # FS1003: "How well did the knowledge, skills, and insights gained
            # in this class align with your expectations?"
            # DR464: "Please provide feedback on the instructor's teaching style,
            # speaking and listening skills, and time management."
            # These question codes cause conflicts with other Skills questions
            "Skills": "skills" in text
            and row["question_code"] not in ["SU122", "FS1003", "DR464"],
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
                f"{row['question_code']} {row['question_text']} contains multiple tags {', '.join([tag for tag, condition in tag_candidates.items() if condition])}. Please adjust the conditions above."
            )

        return next(
            (tag for tag, condition in tag_candidates.items() if condition), None
        )

    evaluation_questions["tag"] = (
        pd.Series(dtype="string")
        if len(evaluation_questions) == 0
        else evaluation_questions.apply(assign_code, axis=1)
    )

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

    # Match ratings with tag - only select necessary columns to reduce memory
    question_tags = evaluation_questions[["question_code", "tag"]].copy()
    evaluation_ratings = evaluation_ratings.merge(
        question_tags, on="question_code", how="left"
    )
    
    # Clean up intermediate data
    del question_tags

    # Get average rating for each course with a specified tag
    def average_by_course(question_tag: str, n_categories: int) -> pd.Series:
        tagged_ratings = evaluation_ratings[evaluation_ratings["tag"] == question_tag]
        # course_id -> Series[list[int]]
        rating_by_course = tagged_ratings.groupby("course_id")[
            ["rating", "question_code"]
        ]
        if len(rating_by_course) == 0:
            return pd.Series()

        def average_rating(data: pd.DataFrame) -> float:
            # A course can have multiple questions of the same type. This usually
            # happens when the course is cross-listed between GS and YC
            weights = [sum(x) for x in zip(*data["rating"])]

            # DR359: How appropriate was the workload? has six options
            # In general, for all other question codes (e.g., YC408)
            # the workload should have 5 categories (n_categories = 5)
            # but this one also qualifies as a "workload" question, so we still assign it
            # the "workload" tag
            if (
                len(weights) != n_categories
                and not (data["question_code"] == "DR359").all()
            ):
                raise database.InvariantError(
                    f"Invalid number of categories for {question_tag}: {len(weights)}"
                )
            if sum(weights) == 0:
                return np.nan
            return cast(float, np.average(range(1, len(weights) + 1), weights=weights))

        return rating_by_course.apply(average_rating)

    # get overall and workload ratings
    evaluation_statistics["avg_rating"] = average_by_course("Overall", 5)
    evaluation_statistics["avg_workload"] = average_by_course("Workload", 5)

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

    # Map course_id to professor_ids
    # use frozenset because it is hashable (set is not), needed for groupby
    course_to_professors = course_professors.groupby("course_id")["professor_id"].apply(
        frozenset
    )

    same_course_id, same_course_to_courses = resolve_historical_courses(
        courses, listings, course_to_professors
    )

    # Split same-course partition by same-professors
    same_course_and_profs_id, same_prof_course_to_courses = split_same_professors(
        same_course_id, course_to_professors
    )

    courses["same_course_id"] = same_course_id
    courses["same_course_and_profs_id"] = same_course_and_profs_id

    logging.debug("Computing last offering statistics")

    # course_id for all evaluated courses
    course_with_enrollment = set(
        evaluation_statistics.dropna(subset=["enrolled"], axis=0).index
    )

    course_to_last_offered: dict[int, int | None] = {}
    course_to_last_enrollment: dict[int, int | None] = {}

    for same_course_group in same_course_to_courses.values():
        ids_by_season = (
            courses.loc[same_course_group]
            .groupby("season_code")
            .apply(lambda x: x.index.tolist())
            .tolist()
        )
        for i, course_ids in enumerate(ids_by_season):
            for course_id in course_ids:
                course_to_last_offered[course_id] = (
                    ids_by_season[i - 1][0] if i > 0 else None
                )
        last_enrollment_id = None
        for i, course_ids in enumerate(ids_by_season):
            for course_id in course_ids:
                course_to_last_enrollment[course_id] = last_enrollment_id
                if course_id in course_with_enrollment:
                    last_enrollment_id = course_id

    courses["last_offered_course_id"] = pd.Series(
        course_to_last_offered, dtype=pd.Int64Dtype()
    )
    courses["last_enrollment_course_id"] = pd.Series(
        course_to_last_enrollment, dtype=pd.Int64Dtype()
    )
    courses["last_enrollment_season_code"] = courses["last_enrollment_course_id"].map(
        courses["season_code"]
    )
    courses["last_enrollment_same_professors"] = course_to_professors.reindex(
        courses.index, fill_value=frozenset()
    ) == courses["last_enrollment_course_id"].map(course_to_professors)
    courses["last_enrollment"] = courses["last_enrollment_course_id"].map(
        evaluation_statistics["enrolled"]
    )

    logging.debug("Computing historical ratings for courses")

    # map courses to ratings - use dictionaries instead of lambda functions for memory efficiency
    course_to_overall = evaluation_statistics["avg_rating"].to_dict()
    course_to_workload = evaluation_statistics["avg_workload"].to_dict()

    # Pre-compute aggregated ratings for each same_course group to avoid repeated list creation
    logging.debug("Pre-computing same-course rating aggregates")
    
    def compute_aggregate_rating(course_ids: list[int], rating_dict: dict[int, float | None]) -> tuple[float | None, int]:
        """Compute average rating directly without creating intermediate lists"""
        ratings = [rating_dict.get(cid) for cid in course_ids]
        ratings = [x for x in ratings if x is not None and not math.isnan(x)]
        if not ratings:
            return None, 0
        return sum(ratings) / len(ratings), len(ratings)
    
    # Build aggregated ratings directly for same_course groups
    same_course_rating_agg = {}
    same_course_workload_agg = {}
    for same_id, course_ids in same_course_to_courses.items():
        same_course_rating_agg[same_id] = compute_aggregate_rating(course_ids, course_to_overall)
        same_course_workload_agg[same_id] = compute_aggregate_rating(course_ids, course_to_workload)
    
    # Build aggregated ratings for same_course_and_profs groups
    same_prof_rating_agg = {}
    same_prof_workload_agg = {}
    for same_prof_id, course_ids in same_prof_course_to_courses.items():
        same_prof_rating_agg[same_prof_id] = compute_aggregate_rating(course_ids, course_to_overall)
        same_prof_workload_agg[same_prof_id] = compute_aggregate_rating(course_ids, course_to_workload)
    
    # Map pre-computed aggregates to courses
    logging.debug("Mapping aggregated ratings to courses")
    courses["average_rating"], courses["average_rating_n"] = zip(
        *courses["same_course_id"].map(same_course_rating_agg)
    )
    courses["average_workload"], courses["average_workload_n"] = zip(
        *courses["same_course_id"].map(same_course_workload_agg)
    )
    courses["average_rating_same_professors"], courses["average_rating_same_professors_n"] = zip(
        *courses["same_course_and_profs_id"].map(same_prof_rating_agg)
    )
    courses["average_workload_same_professors"], courses["average_workload_same_professors_n"] = zip(
        *courses["same_course_and_profs_id"].map(same_prof_workload_agg)
    )
    
    # Convert to proper dtypes
    courses["average_rating_n"] = courses["average_rating_n"].astype(pd.Int64Dtype())
    courses["average_workload_n"] = courses["average_workload_n"].astype(pd.Int64Dtype())
    courses["average_rating_same_professors_n"] = courses["average_rating_same_professors_n"].astype(pd.Int64Dtype())
    courses["average_workload_same_professors_n"] = courses["average_workload_same_professors_n"].astype(pd.Int64Dtype())
    
    # Clean up temporary dictionaries
    del same_course_rating_agg, same_course_workload_agg, same_prof_rating_agg, same_prof_workload_agg
    del course_to_overall, course_to_workload

    courses["average_gut_rating"] = (
        courses["average_rating"] - courses["average_workload"]
    )

    # Calculate average_professor_rating
    courses["average_professor_rating"] = (
        course_professors.merge(
            professors["average_rating"],
            left_on="professor_id",
            right_index=True,
            how="left",
        )
        .groupby("course_id")["average_rating"]
        .mean()
    )

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

    # Build rating dictionary for faster lookup (avoid large merge)
    course_ratings = evaluation_statistics["avg_rating"].to_dict()
    
    # Group by professor first, then lookup ratings - avoids creating huge merged dataframe
    prof_course_groups = course_professors.groupby("professor_id")["course_id"].apply(list)
    
    prof_rating_data = []
    for prof_id, course_ids in prof_course_groups.items():
        # Lookup ratings without creating intermediate lists
        ratings = [course_ratings.get(cid) for cid in course_ids]
        ratings = [r for r in ratings if r is not None and not np.isnan(r)]
        
        if ratings:
            mean = np.mean(ratings)
        else:
            mean = np.nan
        
        prof_rating_data.append({
            "professor_id": prof_id,
            "average_rating": mean,
            "average_rating_n": len(ratings),
        })
    
    prof_to_ratings = pd.DataFrame(prof_rating_data)
    
    # Clean up temporary variables
    del course_ratings, prof_course_groups

    professors = (
        professors.reset_index()
        .merge(prof_to_ratings, how="left", on="professor_id")
        .set_index("professor_id")
    )
    professors["average_rating_n"] = professors["average_rating_n"].astype(
        pd.Int64Dtype()
    )

    # Compute courses_taught more efficiently using value_counts
    professors["courses_taught"] = (
        course_professors["professor_id"]
        .value_counts()
        .reindex(professors.index, fill_value=0)
        .astype(int)
    )

    return professors

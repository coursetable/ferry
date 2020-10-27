import csv
from typing import List

import pandas as pd
import ujson

from ferry import config, database

QUESTION_TAGS = dict()
with open(f"{config.RESOURCE_DIR}/question_tags.csv") as f:
    for question_code, tag in csv.reader(f):
        QUESTION_TAGS[question_code] = tag


def questions_computed(evaluation_questions):
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

def evaluation_statistics_computed(evaluation_statistics, evaluation_ratings, evaluation_questions):

    question_code_map = dict(zip(evaluation_questions["question_code"],evaluation_questions["tag"]))

    evaluation_ratings = evaluation_ratings.copy(deep=True)
    evaluation_ratings["tag"] = evaluation_ratings["question_code"].apply(question_code_map.get)

    def average_rating(ratings: List[int]) -> float:
        if not ratings or not sum(ratings):
            return None
        agg = 0
        for i, rating in enumerate(ratings):
            multiplier = i + 1
            agg += multiplier * rating
        return agg / sum(ratings)

    def average_by_course(tag, n_categories):

        tagged_ratings = evaluation_ratings[evaluation_ratings["tag"]==tag].copy(deep=True)
        rating_by_course = tagged_ratings.groupby("course_id")["rating"].apply(list)

        # Aggregate responses across question variants.
        rating_by_course = rating_by_course.apply(lambda data: [sum(x) for x in zip(*data)])

        lengths_invalid = rating_by_course.apply(len) != n_categories

        if any(lengths_invalid):
            raise database.InvariantError(
                f"Invalid workload responses, expected length of 5: {rating_by_course[lengths_invalid]}"
            )

        rating_by_course = rating_by_course.apply(average_rating)
        rating_by_course = rating_by_course.to_dict()

        return rating_by_course

    overall_by_course = average_by_course("rating", 5)
    workload_by_course = average_by_course("workload", 5)

    evaluation_statistics["avg_rating"] = evaluation_statistics["course_id"].apply(overall_by_course.get)
    evaluation_statistics["avg_workload"] = evaluation_statistics["course_id"].apply(workload_by_course.get)

    return evaluation_statistics

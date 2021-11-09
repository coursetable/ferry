"""
Loads the ratings JSON files from fetch_ratings.py
and compiles them into tables for questions, ratings, statistics,
and narratives for input into transform.py. It also computes
sentiment intensity estimates over the narratives with VADER.
"""

import csv
from pathlib import Path
from typing import List

import pandas as pd
import ujson

from ferry import config
from ferry.includes.rating_parsing import (
    process_narratives,
    process_questions,
    process_ratings,
    process_statistics,
)
from ferry.includes.tqdm import tqdm

config.init_sentry()

# ------------------
# CSV output headers
# ------------------

questions_headers = [
    "season",
    "crn",
    "question_code",
    "is_narrative",
    "question_text",
    "options",
]
ratings_headers = ["season", "crn", "question_code", "rating"]
statistics_headers = [
    "season",
    "crn",
    "enrolled",
    "responses",
    "declined",
    "no_response",
    "extras",
]
narratives_headers = [
    "season",
    "crn",
    "question_code",
    "comment",
    "comment_neg",
    "comment_neu",
    "comment_pos",
    "comment_compound",
]

# ----------------
# CSV output paths
# ----------------

questions_path = config.DATA_DIR / "parsed_evaluations/evaluation_questions.csv"
ratings_path = config.DATA_DIR / "parsed_evaluations/evaluation_ratings.csv"
statistics_path = config.DATA_DIR / "parsed_evaluations/evaluation_statistics.csv"
narratives_path = config.DATA_DIR / "parsed_evaluations/evaluation_narratives.csv"

# ------------------
# CSV output writers
# ------------------

questions_file = open(questions_path, "w")  # pylint: disable=consider-using-with
questions_writer = csv.DictWriter(questions_file, questions_headers)
questions_writer.writeheader()

narratives_file = open(narratives_path, "w")  # pylint: disable=consider-using-with
narratives_writer = csv.DictWriter(narratives_file, narratives_headers)
narratives_writer.writeheader()

ratings_file = open(ratings_path, "w")  # pylint: disable=consider-using-with
ratings_writer = csv.DictWriter(ratings_file, ratings_headers)
ratings_writer.writeheader()

statistics_file = open(statistics_path, "w")  # pylint: disable=consider-using-with
statistics_writer = csv.DictWriter(statistics_file, statistics_headers)
statistics_writer.writeheader()


if __name__ == "__main__":

    # ----------------------------
    # Load and process evaluations
    # ----------------------------

    # list available evaluation files
    previous_eval_files = Path(config.DATA_DIR / "previous_evals").glob("*.json")
    new_eval_files = Path(config.DATA_DIR / "course_evals").glob("*.json")

    # extract file names (<season> + <crn> format) for merging
    previous_eval_filenames = [x.name for x in previous_eval_files]
    new_eval_filenames = [x.name for x in new_eval_files]

    all_eval_files = sorted(list(set(previous_eval_filenames + new_eval_filenames)))

    merged_evaluations: List[pd.DataFrame] = []

    for filename in tqdm(all_eval_files, desc="Processing evaluations"):
        # Read the evaluation, giving preference to current over previous.
        current_evals_file = Path(f"{config.DATA_DIR}/course_evals/{filename}")

        if current_evals_file.is_file():
            with open(current_evals_file, "r") as f:
                evaluation = ujson.load(f)
        else:
            with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
                evaluation = ujson.load(f)

        process_narratives(evaluation, narratives_writer)
        process_ratings(evaluation, ratings_writer)
        process_questions(evaluation, questions_writer)
        process_statistics(evaluation, statistics_writer)

    # close CSV files
    questions_file.close()
    narratives_file.close()
    ratings_file.close()
    statistics_file.close()

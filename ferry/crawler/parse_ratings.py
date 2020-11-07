import csv
from pathlib import Path

import pandas as pd
import ujson
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

from ferry import config
from ferry.includes.tqdm import tqdm

"""
=================================================================
This script loads the ratings JSON files from fetch_ratings.py
and compiles them into tables for questions, ratings, statistics,
and narratives for input into transform.py. It also computes
sentiment intensity estimates over the narratives with VADER.
=================================================================
"""

# initialize sentiment intensity analyzer
analyzer = SentimentIntensityAnalyzer()

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

questions_file = open(questions_path, "w")
questions_writer = csv.DictWriter(questions_file, questions_headers)
questions_writer.writeheader()

narratives_file = open(narratives_path, "w")
narratives_writer = csv.DictWriter(narratives_file, narratives_headers)
narratives_writer.writeheader()

ratings_file = open(ratings_path, "w")
ratings_writer = csv.DictWriter(ratings_file, ratings_headers)
ratings_writer.writeheader()

statistics_file = open(statistics_path, "w")
statistics_writer = csv.DictWriter(statistics_file, statistics_headers)
statistics_writer.writeheader()


def process_narratives(evaluation):

    for narrative_group in evaluation["narratives"]:

        for raw_narrative in narrative_group["comments"]:

            narrative = {}
            narrative["season"] = evaluation["season"]
            narrative["crn"] = evaluation["crn_code"]
            narrative["question_code"] = narrative_group["question_id"]
            narrative["comment"] = raw_narrative

            sentiment = analyzer.polarity_scores(raw_narrative)

            narrative["comment_neg"] = sentiment["neg"]
            narrative["comment_neu"] = sentiment["neu"]
            narrative["comment_pos"] = sentiment["pos"]
            narrative["comment_compound"] = sentiment["compound"]

            narratives_writer.writerow(narrative)


def process_ratings(evaluation):

    for raw_rating in evaluation["ratings"]:
        rating = {}

        rating["season"] = evaluation["season"]
        rating["crn"] = evaluation["crn_code"]
        rating["question_code"] = raw_rating["question_id"]
        rating["rating"] = ujson.dumps(raw_rating["data"])

        ratings_writer.writerow(rating)


def process_statistics(evaluation):

    statistics = {}
    statistics["season"] = evaluation["season"]
    statistics["crn"] = evaluation["crn_code"]
    statistics["enrolled"] = evaluation["enrollment"]["enrolled"]
    statistics["responses"] = evaluation["enrollment"]["responses"]
    statistics["declined"] = evaluation["enrollment"]["declined"]
    statistics["no_response"] = evaluation["enrollment"]["no response"]
    statistics["extras"] = evaluation["extras"]

    statistics_writer.writerow(statistics)


def process_questions(evaluation):
    for rating in evaluation["ratings"]:
        question = {}
        question["season"] = evaluation["season"]
        question["crn"] = evaluation["crn_code"]
        question["question_code"] = rating["question_id"]
        question["question_text"] = rating["question_text"]
        question["is_narrative"] = False
        question["options"] = ujson.dumps(rating["options"])

        questions_writer.writerow(question)

    for narrative in evaluation["narratives"]:

        question = {}
        question["season"] = evaluation["season"]
        question["crn"] = evaluation["crn_code"]
        question["question_code"] = narrative["question_id"]
        question["question_text"] = narrative["question_text"]
        question["is_narrative"] = True
        question["options"] = None

        questions_writer.writerow(question)


def process_evaluation(evaluation):

    process_narratives(evaluation)
    process_ratings(evaluation)
    process_statistics(evaluation)
    process_questions(evaluation)

    return


# ----------------------------
# Load and process evaluations
# ----------------------------

# list available evaluation files
previous_eval_files = Path(config.DATA_DIR / "previous_evals").glob("*.json")
new_eval_files = Path(config.DATA_DIR / "course_evals").glob("*.json")

# extract file names (<season> + <crn> format) for merging
previous_eval_files = [x.name for x in previous_eval_files]
new_eval_files = [x.name for x in new_eval_files]

all_eval_files = sorted(list(set(previous_eval_files + new_eval_files)))

merged_evaluations = []

for filename in tqdm(all_eval_files, desc="Processing evaluations"):
    # Read the evaluation, giving preference to current over previous.
    current_evals_file = Path(f"{config.DATA_DIR}/course_evals/{filename}")

    if current_evals_file.is_file():
        with open(current_evals_file, "r") as f:
            evaluation = ujson.load(f)
    else:
        with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
            evaluation = ujson.load(f)

    process_evaluation(evaluation)

# close CSV files
questions_file.close()
narratives_file.close()
ratings_file.close()
statistics_file.close()

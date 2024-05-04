"""
Functions for processing course rating JSONs into aggregate CSVs.

Used by /ferry/crawler/parse_ratings.py
"""

import csv
from typing import Any

import ujson
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# initialize sentiment intensity analyzer
analyzer = SentimentIntensityAnalyzer()
import asyncio
import concurrent.futures
import csv
from pathlib import Path

import ujson
from tqdm.asyncio import tqdm_asyncio

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


def parse_rating(
    data_dir: Path,
    filename: str,
):
    # Read the evaluation, giving preference to current over previous.
    current_evals_file = data_dir / "course_evals" / filename
    previous_evals_file = data_dir / "previous_evals" / filename

    if current_evals_file.is_file():
        with open(current_evals_file, "r") as f:
            evaluation = ujson.load(f)
    else:
        with open(previous_evals_file, "r") as f:
            evaluation = ujson.load(f)

    return (
        process_narratives(evaluation),
        process_ratings(evaluation),
        process_questions(evaluation),
        process_statistics(evaluation),
    )


async def parse_ratings(data_dir: Path):

    print(f"Parsing course ratings...")
    parsed_evaluations_dir = data_dir / "parsed_evaluations"
    parsed_evaluations_dir.mkdir(parents=True, exist_ok=True)
    questions_path = parsed_evaluations_dir / "evaluation_questions.csv"
    ratings_path = parsed_evaluations_dir / "evaluation_ratings.csv"
    statistics_path = parsed_evaluations_dir / "evaluation_statistics.csv"
    narratives_path = parsed_evaluations_dir / "evaluation_narratives.csv"

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

    # ----------------------------
    # Load and process evaluations
    # ----------------------------

    # list available evaluation files
    previous_eval_files = (data_dir / "previous_evals").glob("*.json")
    new_eval_files = (data_dir / "course_evals").glob("*.json")

    # extract file names (<season> + <crn> format) for merging
    previous_eval_filenames = [x.name for x in previous_eval_files]
    new_eval_filenames = [x.name for x in new_eval_files]

    all_eval_files = sorted(list(set(previous_eval_filenames + new_eval_filenames)))

    with concurrent.futures.ProcessPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(
                executor,
                parse_rating,
                data_dir,
                filename,
            )
            for filename in all_eval_files
        ]
        results: list[tuple[list, list, list, dict]] = await tqdm_asyncio.gather(
            *futures, leave=False, desc=f"Parsing all ratings"
        )
        # results: [ ( narratives: List, ratings: List, questions: List, statistics: Dict ) ]

    # Must be done in serial, as the CSV writers are not thread-safe.
    for narratives, ratings, questions, statistics in results:
        # write narratives to CSV
        narratives_writer.writerows(narratives)

        # write ratings to CSV
        ratings_writer.writerows(ratings)

        # write questions to CSV
        questions_writer.writerows(questions)

        # write statistics to CSV
        statistics_writer.writerow(statistics)

    # close CSV files
    questions_file.close()
    narratives_file.close()
    ratings_file.close()
    statistics_file.close()

    print("\033[F", end="")
    print(f"Parsing course ratings... ✔")


####################
# Helper Functions #
####################


def process_narratives(evaluation: dict[str, Any]):
    """
    Process written evaluations. Appends to narratives CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    narratives_writer:
        CSV writer to narratives output file.
    """
    narratives = []
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

            narratives.append(narrative)

    return narratives


def process_ratings(evaluation: dict[str, Any]):
    """
    Process categorical evaluations. Appends to ratings CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    ratings_writer:
        CSV writer to ratings output file.
    """
    ratings = []
    for raw_rating in evaluation["ratings"]:
        rating = {}

        rating["season"] = evaluation["season"]
        rating["crn"] = evaluation["crn_code"]
        rating["question_code"] = raw_rating["question_id"]
        rating["rating"] = ujson.dumps(raw_rating["data"])

        ratings.append(rating)

    return ratings


def process_statistics(evaluation: dict[str, Any]):
    """
    Process evaluation statistics. Appends to statistics CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    statistics_writer:
        CSV writer to course statistics output file.
    """
    statistics = {}
    statistics["season"] = evaluation["season"]
    statistics["crn"] = evaluation["crn_code"]
    statistics["enrolled"] = evaluation["enrollment"]["enrolled"]
    statistics["responses"] = evaluation["enrollment"]["responses"]
    statistics["declined"] = evaluation["enrollment"]["declined"]
    statistics["no_response"] = evaluation["enrollment"]["no response"]
    statistics["extras"] = evaluation["extras"]

    return statistics


def process_questions(evaluation: dict[str, Any]):
    """
    Process evaluation questions. Appends to questions CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    questions_writer:
        CSV writer to questions output file.
    """
    questions = []

    for rating in evaluation["ratings"]:
        question = {}
        question["season"] = evaluation["season"]
        question["crn"] = evaluation["crn_code"]
        question["question_code"] = rating["question_id"]
        question["question_text"] = rating["question_text"]
        question["is_narrative"] = False
        question["options"] = ujson.dumps(rating["options"])
        questions.append(question)

    for narrative in evaluation["narratives"]:
        question = {}
        question["season"] = evaluation["season"]
        question["crn"] = evaluation["crn_code"]
        question["question_code"] = narrative["question_id"]
        question["question_text"] = narrative["question_text"]
        question["is_narrative"] = True
        question["options"] = None
        questions.append(question)

    return questions

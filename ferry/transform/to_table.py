"""
Functions for processing course rating JSONs into aggregate CSVs.

TODO: do we need this step at all?
"""

import csv
from typing import cast

import ujson

import asyncio
import concurrent.futures
import csv
from pathlib import Path

import ujson
from tqdm.asyncio import tqdm_asyncio
from ferry.crawler.evals.parse import ParsedEval


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
]


async def create_evals_tables(data_dir: Path):
    print(f"Parsing course ratings...")
    parsed_evaluations_dir = data_dir / "parsed_evaluations"
    parsed_evaluations_dir.mkdir(parents=True, exist_ok=True)
    questions_path = parsed_evaluations_dir / "evaluation_questions.csv"
    ratings_path = parsed_evaluations_dir / "evaluation_ratings.csv"
    narratives_path = parsed_evaluations_dir / "evaluation_narratives.csv"
    statistics_path = parsed_evaluations_dir / "evaluation_statistics.csv"

    questions_file = open(questions_path, "w")  # pylint: disable=consider-using-with
    questions_writer = csv.DictWriter(questions_file, questions_headers)
    questions_writer.writeheader()

    ratings_file = open(ratings_path, "w")  # pylint: disable=consider-using-with
    ratings_writer = csv.DictWriter(ratings_file, ratings_headers)
    ratings_writer.writeheader()

    narratives_file = open(narratives_path, "w")  # pylint: disable=consider-using-with
    narratives_writer = csv.DictWriter(narratives_file, narratives_headers)
    narratives_writer.writeheader()

    statistics_file = open(statistics_path, "w")  # pylint: disable=consider-using-with
    statistics_writer = csv.DictWriter(statistics_file, statistics_headers)
    statistics_writer.writeheader()

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
            loop.run_in_executor(executor, create_rating_table_row, data_dir, filename)
            for filename in all_eval_files
        ]
        results: list[tuple[list, list, list, dict]] = await tqdm_asyncio.gather(
            *futures, leave=False, desc=f"Parsing all ratings"
        )

    # Must be done in serial, as the CSV writers are not thread-safe.
    for questions, ratings, narratives, statistics in results:
        questions_writer.writerows(questions)
        ratings_writer.writerows(ratings)
        narratives_writer.writerows(narratives)
        statistics_writer.writerow(statistics)

    questions_file.close()
    ratings_file.close()
    narratives_file.close()
    statistics_file.close()

    print("\033[F", end="")
    print(f"Parsing course ratings... ✔")


def create_rating_table_row(data_dir: Path, filename: str):
    # Read the evaluation, giving preference to current over previous.
    current_evals_file = data_dir / "course_evals" / filename
    previous_evals_file = data_dir / "previous_evals" / filename

    if current_evals_file.is_file():
        with open(current_evals_file, "r") as f:
            evaluation = cast(ParsedEval, ujson.load(f))
    else:
        with open(previous_evals_file, "r") as f:
            evaluation = cast(ParsedEval, ujson.load(f))

    return (
        process_questions(evaluation),
        process_ratings(evaluation),
        process_narratives(evaluation),
        process_statistics(evaluation),
    )


def process_narratives(evaluation: ParsedEval):
    narratives = []
    for narrative_group in evaluation["narratives"]:
        for raw_narrative in narrative_group["comments"]:
            narratives.append(
                {
                    "season": evaluation["season"],
                    "crn": evaluation["crn_code"],
                    "question_code": narrative_group["question_id"],
                    "comment": raw_narrative,
                }
            )

    return narratives


def process_ratings(evaluation: ParsedEval):
    return [
        {
            "season": evaluation["season"],
            "crn": evaluation["crn_code"],
            "question_code": rating["question_id"],
            "rating": ujson.dumps(rating["data"]),
        }
        for rating in evaluation["ratings"]
    ]


def process_statistics(evaluation: ParsedEval):
    return {
        "season": evaluation["season"],
        "crn": evaluation["crn_code"],
        "enrolled": evaluation["enrollment"]["enrolled"],
        "responses": evaluation["enrollment"]["responses"],
        "declined": evaluation["enrollment"]["declined"],
        "no_response": evaluation["enrollment"]["no response"],
        "extras": evaluation["extras"],
    }


def process_questions(evaluation: ParsedEval):
    questions = []

    for rating in evaluation["ratings"]:
        questions.append(
            {
                "season": evaluation["season"],
                "crn": evaluation["crn_code"],
                "question_code": rating["question_id"],
                "question_text": rating["question_text"],
                "is_narrative": False,
                "options": ujson.dumps(rating["options"]),
            }
        )

    for narrative in evaluation["narratives"]:
        questions.append(
            {
                "season": evaluation["season"],
                "crn": evaluation["crn_code"],
                "question_code": narrative["question_id"],
                "question_text": narrative["question_text"],
                "is_narrative": True,
                "options": None,
            }
        )

    return questions

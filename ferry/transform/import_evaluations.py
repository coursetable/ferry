import logging
from pathlib import Path
import itertools
import re

import asyncio
import concurrent.futures
from tqdm.asyncio import tqdm_asyncio
import numpy as np
import pandas as pd
import ujson
from typing import cast, TypedDict

from ferry import database
from ferry.crawler.evals.parse import ParsedEval


def match_evaluations_to_courses(
    evaluation_narratives: pd.DataFrame,
    evaluation_ratings: pd.DataFrame,
    evaluation_statistics: pd.DataFrame,
    listings: pd.DataFrame,
) -> tuple[pd.DataFrame, ...]:
    logging.debug("Matching evaluations to courses")

    # construct outer season grouping
    season_crn_to_course_id: dict[str, dict[str, float]] = (
        listings[["season_code", "course_id", "crn"]]
        .groupby("season_code")
        .apply(
            lambda x: x[["crn", "course_id"]]
            .set_index("crn")["course_id"]
            .astype(float)
            .to_dict()
        )
        .to_dict()
    )

    def get_course_id(row: pd.Series):
        course_id = season_crn_to_course_id.get(row["season"], {}).get(
            row["crn"], np.nan
        )
        return course_id

    # get course IDs
    evaluation_narratives["course_id"] = evaluation_narratives.apply(
        get_course_id, axis=1
    )
    evaluation_ratings["course_id"] = evaluation_ratings.apply(get_course_id, axis=1)
    evaluation_statistics["course_id"] = evaluation_statistics.apply(
        get_course_id, axis=1
    )

    # each course must have exactly one statistic, so use this for reporting
    nan_total = evaluation_statistics["course_id"].isna().sum()
    logging.debug(
        f"Removing {nan_total}/{len(evaluation_statistics)} evaluated courses without matches"
    )

    # remove unmatched courses
    evaluation_narratives.dropna(subset=["course_id"], axis=0, inplace=True)
    evaluation_ratings.dropna(subset=["course_id"], axis=0, inplace=True)
    evaluation_statistics.dropna(subset=["course_id"], axis=0, inplace=True)

    # change from float to integer type for import
    evaluation_narratives["course_id"] = evaluation_narratives["course_id"].astype(int)
    evaluation_ratings["course_id"] = evaluation_ratings["course_id"].astype(int)
    evaluation_statistics["course_id"] = evaluation_statistics["course_id"].astype(int)

    return evaluation_narratives, evaluation_ratings, evaluation_statistics


class EvalTables(TypedDict):
    evaluation_narratives: pd.DataFrame
    evaluation_ratings: pd.DataFrame
    evaluation_statistics: pd.DataFrame
    evaluation_questions: pd.DataFrame


def create_rating_table_row(data_dir: Path, filename: str):
    with open(data_dir / "parsed_evaluations" / filename, "r") as f:
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
                    "crn": int(evaluation["crn_code"]),
                    "question_code": narrative_group["question_id"],
                    "comment": raw_narrative,
                }
            )

    return narratives


def process_ratings(evaluation: ParsedEval):
    return [
        {
            "season": evaluation["season"],
            "crn": int(evaluation["crn_code"]),
            "question_code": rating["question_id"],
            "rating": rating["data"],
        }
        for rating in evaluation["ratings"]
    ]


def process_statistics(evaluation: ParsedEval):
    return {
        "season": evaluation["season"],
        "crn": int(evaluation["crn_code"]),
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
                "crn": int(evaluation["crn_code"]),
                "question_code": rating["question_id"],
                "question_text": rating["question_text"],
                "is_narrative": False,
                "options": rating["options"],
            }
        )

    for narrative in evaluation["narratives"]:
        questions.append(
            {
                "season": evaluation["season"],
                "crn": int(evaluation["crn_code"]),
                "question_code": narrative["question_id"],
                "question_text": narrative["question_text"],
                "is_narrative": True,
                "options": None,
            }
        )

    return questions


async def import_evaluations(data_dir: Path, listings: pd.DataFrame) -> EvalTables:
    """
    Import evaluations from JSON files in `data_dir`.
    Splits the raw data into various tables for the database.

    Returns
    -------
    evaluation_narratives,
    evaluation_ratings,
    evaluation_statistics,
    evaluation_questions
    """
    print("\nImporting course evaluations...")
    eval_filenames = sorted(
        [x.name for x in (data_dir / "parsed_evaluations").glob("*.json")]
    )

    with concurrent.futures.ProcessPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(executor, create_rating_table_row, data_dir, filename)
            for filename in eval_filenames
        ]
        results: list[tuple[list, list, list, dict]] = await tqdm_asyncio.gather(
            *futures, leave=False, desc=f"Parsing all ratings"
        )

    table_rows = list(zip(*results))
    evaluation_questions = pd.DataFrame(itertools.chain(*table_rows[0]))
    evaluation_ratings = pd.DataFrame(itertools.chain(*table_rows[1]))
    evaluation_narratives = pd.DataFrame(itertools.chain(*table_rows[2]))
    evaluation_statistics = pd.DataFrame(table_rows[3])
    evaluation_statistics[["enrolled", "responses", "declined", "no_response"]] = (
        evaluation_statistics[
            ["enrolled", "responses", "declined", "no_response"]
        ].astype(pd.Int64Dtype())
    )

    (
        evaluation_narratives,
        evaluation_ratings,
        evaluation_statistics,
    ) = match_evaluations_to_courses(
        evaluation_narratives=evaluation_narratives,
        evaluation_ratings=evaluation_ratings,
        evaluation_statistics=evaluation_statistics,
        listings=listings,
    )
    # drop cross-listing duplicates
    evaluation_statistics.drop_duplicates(
        subset=["course_id"], inplace=True, keep="first"
    )
    evaluation_ratings.drop_duplicates(
        subset=["course_id", "question_code"], inplace=True, keep="first"
    )
    evaluation_narratives.drop_duplicates(
        subset=["course_id", "question_code", "comment"],
        inplace=True,
        keep="first",
    )

    # -------------------
    # Aggregate questions
    # -------------------
    # Normalize question texts
    def amend_text(text: str):
        text = text.replace(
            "(Your anonymous response to this question may be viewed by Yale College students, faculty, and advisers to aid in course selection and evaluating teaching.)",
            "",
        )
        text = re.sub(r"[ \t\r\n]+", " ", text)
        text = re.sub(r"</?[a-z]+>", "", text)
        text = re.sub(r" *(Comments|Ratings):$", "", text)
        text = text.replace("course or module", "course or workshop")
        text = text.replace("YSE", "F&ES")
        return text.strip()

    evaluation_questions["question_text"] = evaluation_questions["question_text"].apply(
        amend_text
    )

    # consistency checks
    logging.debug("Checking question text consistency")
    text_by_code = cast(
        pd.Series,
        evaluation_questions.groupby("question_code")["question_text"].apply(
            lambda x: set(x) - {""}
        ),
    )

    def amend_text_group(texts: set[str]) -> set[str]:
        # Old DR questions just say "Ratings:" which now have the actual question included
        if len(texts) == 2:
            text_1, text_2 = texts
            if len(text_1) > len(text_2):
                text_1, text_2 = text_2, text_1
            if text_2.endswith(text_1) or text_2.startswith(text_1):
                return {text_2}
        if len(texts) == 0:
            return {""}
        return texts

    text_by_code = text_by_code.apply(amend_text_group)

    diverging_texts = text_by_code[text_by_code.apply(len) > 1]
    if not diverging_texts.empty:
        print(diverging_texts)
        raise database.InvariantError("Diverging question texts")

    text_by_code = text_by_code.apply(lambda x: next(iter(x)))
    evaluation_questions["question_text"] = evaluation_questions["question_code"].map(
        text_by_code
    )

    logging.debug("Checking question type (narrative/rating) consistency")
    is_narrative_by_code = evaluation_questions.groupby("question_code")[
        "is_narrative"
    ].apply(set)

    # check that a question code is always narrative or always rating
    if not all(is_narrative_by_code.apply(len) == 1):
        inconsistent_codes = ", ".join(
            [
                str(x)
                for x in is_narrative_by_code[
                    is_narrative_by_code.apply(len) != 1
                ].index
            ]
        )
        raise database.InvariantError(
            f"Error: question codes {inconsistent_codes} have both narratives and ratings"
        )

    # deduplicate questions and keep most recent
    evaluation_questions = evaluation_questions.sort_values(
        by="season", ascending=False
    )
    evaluation_questions.drop_duplicates(
        subset=["question_code"], keep="first", inplace=True
    )

    evaluation_questions["options"] = evaluation_questions["options"].replace(
        "NaN", "[]"
    )

    # evaluation narratives ----------------

    # filter out missing or short comments
    evaluation_narratives.dropna(subset=["comment"], inplace=True)

    # MIN_COMMENT_LENGTH = 2
    evaluation_narratives = evaluation_narratives.loc[
        evaluation_narratives["comment"].apply(len) > 2
    ].reset_index(drop=True)

    evaluation_narratives["response_number"] = evaluation_narratives.groupby(
        ["course_id", "question_code"]
    ).cumcount()
    # id column for database primary key
    evaluation_narratives.index.rename("id", inplace=True)

    # evaluation ratings ----------------

    # id column for database primary key
    evaluation_ratings.reset_index(drop=True, inplace=True)
    evaluation_ratings.index.rename("id", inplace=True)

    # evaluation questions ----------------
    evaluation_questions.reset_index(drop=True, inplace=True)

    # evaluation statistics ----------------

    # convert to JSON string for postgres
    evaluation_statistics.loc[:, "extras"] = evaluation_statistics["extras"].apply(
        ujson.dumps
    )
    evaluation_statistics.set_index("course_id", inplace=True)

    print("\033[F", end="")
    print("Importing course evaluations... ✔")

    print("[Summary]")
    print(f"Total evaluation narratives: {len(evaluation_narratives)}")
    print(f"Total evaluation ratings: {len(evaluation_ratings)}")
    print(f"Total evaluation statistics: {len(evaluation_statistics)}")
    print(f"Total evaluation questions: {len(evaluation_questions)}")

    return {
        "evaluation_narratives": evaluation_narratives,
        "evaluation_ratings": evaluation_ratings,
        "evaluation_statistics": evaluation_statistics,
        "evaluation_questions": evaluation_questions,
    }

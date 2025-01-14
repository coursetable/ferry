import logging
from pathlib import Path
import re

from tqdm import tqdm
import numpy as np
import pandas as pd
import ujson
from typing import cast, TypedDict

from ferry import database


def match_evaluations_to_courses(
    evals: pd.DataFrame, listings: pd.DataFrame
) -> pd.DataFrame:
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

    evals["course_id"] = evals.apply(get_course_id, axis=1)

    nan_total = evals["course_id"].isna().sum()
    logging.debug(
        f"Removing {nan_total}/{len(evals)} evaluated courses without matches"
    )

    # remove unmatched courses
    evals.dropna(subset=["course_id"], axis=0, inplace=True)
    # change from float to integer type for import
    evals["course_id"] = evals["course_id"].astype(int)
    return evals


class EvalTables(TypedDict):
    evaluation_narratives: pd.DataFrame
    evaluation_ratings: pd.DataFrame
    evaluation_statistics: pd.DataFrame
    evaluation_questions: pd.DataFrame


def import_evaluations(data_dir: Path, listings: pd.DataFrame) -> EvalTables:
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
    parsed_evals_dir = data_dir / "parsed_evaluations"
    eval_filenames = sorted([x.name for x in parsed_evals_dir.glob("*.json")])
    all_imported_evals: list[pd.DataFrame] = []
    for filename in tqdm(eval_filenames, desc="Loading eval JSONs", leave=False):
        parsed_evals_file = parsed_evals_dir / filename
        parsed_course_info = pd.read_json(
            parsed_evals_file,
            dtype={
                "crn": int,
                "season": str,
                "enrolled": pd.Int64Dtype(),
                "responses": pd.Int64Dtype(),
            },
        )
        all_imported_evals.append(parsed_course_info)

    courses = pd.concat(all_imported_evals, axis=0, ignore_index=True)
    # Delete variables to avoid OOM
    del all_imported_evals
    courses = match_evaluations_to_courses(courses, listings)
    evaluation_statistics = courses[
        ["season", "course_id", "enrolled", "responses", "extras"]
    ].copy()
    rating_qa = (
        courses.drop(columns=["enrolled", "responses", "extras", "narratives"])
        .explode(column="ratings")
        .dropna(subset=["ratings"])
    )
    narrative_qa = (
        courses.drop(columns=["enrolled", "responses", "extras", "ratings"])
        .explode(column="narratives")
        .dropna(subset=["narratives"])
    )
    del courses
    print(rating_qa[rating_qa["ratings"].apply(lambda x: x["question_code"] == "DR155")])
    print(rating_qa.head(10))
    rating_qa[["question_code", "question_text", "options", "data"]] = (
        pd.DataFrame(list(rating_qa["ratings"]), index=rating_qa.index)
    )
    rating_qa.drop(columns=["ratings"], inplace=True)
    print(rating_qa.head(10))
    print(rating_qa[rating_qa["question_code"] == "DR155"])
    evaluation_ratings = rating_qa[
        ["season", "course_id", "question_code", "data"]
    ].rename(columns={"data": "rating"})
    rating_questions = rating_qa[
        ["season", "question_code", "question_text", "options"]
    ].copy()
    del rating_qa
    narrative_qa[["question_code", "question_text", "comments"]] = pd.DataFrame(
        list(narrative_qa["narratives"]), index=narrative_qa.index
    )
    narrative_qa.drop(columns=["narratives"], inplace=True)
    evaluation_narratives = (
        narrative_qa[["season", "course_id", "question_code", "comments"]]
        .explode(column="comments")
        .rename(columns={"comments": "comment"})
    )
    narrative_questions = narrative_qa[
        ["season", "question_code", "question_text"]
    ].copy()
    del narrative_qa
    narrative_questions["options"] = None
    rating_questions["is_narrative"] = False
    narrative_questions["is_narrative"] = True
    evaluation_questions = pd.concat(
        [rating_questions, narrative_questions], axis=0, ignore_index=True
    )

    # drop cross-listing duplicates
    # Note: do *not* drop duplicates by each course_id, because different
    # listings can have different questions (e.g. YC and GS)
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

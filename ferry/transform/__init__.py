import os
from pathlib import Path
from typing import cast

import pandas as pd

from ferry import database
from .transform_compute import (
    courses_computed,
    evaluation_statistics_computed,
    professors_computed,
    questions_computed,
    narratives_computed,
)
from .import_courses import import_courses
from .import_evaluations import import_evaluations


def write_csvs(tables: dict[str, pd.DataFrame], data_dir: Path):
    print("\nWriting tables to disk as CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    for table_name, table in tables.items():
        cast(pd.DataFrame, table).to_csv(csv_dir / f"{table_name}.csv")

    print("\033[F", end="")
    print("Writing tables to disk as CSVs... ✔")


def transform(data_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Import the parsed course and evaluation data into CSVs generated with Pandas.

    Used immediately before stage.py as the first step in the import process.
    """

    # get full list of course seasons from files
    course_seasons = sorted(
        [
            filename.split(".")[0]  # remove the .json extension
            for filename in os.listdir(data_dir / "parsed_courses")
            if filename[0] != "."
        ]
    )
    print(f"\nSeason(s): {', '.join(course_seasons)}")
    seasons_table = pd.DataFrame(
        [
            [x, {"1": "spring", "2": "summer", "3": "fall"}[x[-1]], int(x[:4])]
            for x in course_seasons
        ],
        columns=["season_code", "term", "year"],
    )

    course_tables = import_courses(data_dir / "parsed_courses", course_seasons)
    eval_tables = import_evaluations(
        data_dir / "evaluation_tables", course_tables["listings"]
    )

    print("\nComputing secondary attributes...")

    eval_tables["evaluation_questions"] = questions_computed(
        eval_tables["evaluation_questions"]
    )

    eval_tables["evaluation_narratives"] = narratives_computed(
        eval_tables["evaluation_narratives"]
    )

    eval_tables["evaluation_statistics"] = evaluation_statistics_computed(
        evaluation_statistics=eval_tables["evaluation_statistics"],
        evaluation_ratings=eval_tables["evaluation_ratings"],
        evaluation_questions=eval_tables["evaluation_questions"],
    )

    course_tables["professors"] = professors_computed(
        professors=course_tables["professors"],
        course_professors=course_tables["course_professors"],
        evaluation_statistics=eval_tables["evaluation_statistics"],
    )

    course_tables["courses"] = courses_computed(
        courses=course_tables["courses"],
        listings=course_tables["listings"],
        evaluation_statistics=eval_tables["evaluation_statistics"],
        course_professors=course_tables["course_professors"],
        professors=course_tables["professors"],
    )

    all_tables = {"seasons": seasons_table, **course_tables, **eval_tables}

    # Remove intermediate columns to match DB schema
    for table_name, db_table in [
        ("seasons", database.Season.__table__),
        ("courses", database.Course.__table__),
        ("listings", database.Listing.__table__),
        ("course_professors", database.course_professors),
        ("professors", database.Professor.__table__),
        ("flags", database.Flag.__table__),
        ("course_flags", database.course_flags),
        ("evaluation_questions", database.EvaluationQuestion.__table__),
        ("evaluation_narratives", database.EvaluationNarrative.__table__),
        ("evaluation_statistics", database.EvaluationStatistics.__table__),
        ("evaluation_ratings", database.EvaluationRating.__table__),
    ]:
        all_tables[table_name] = (
            all_tables[table_name]
            .reset_index(drop=False)
            .loc[:, [column.key for column in db_table.columns]]
        )

    print("\033[F", end="")
    print("Computing secondary attributes... ✔")

    return all_tables

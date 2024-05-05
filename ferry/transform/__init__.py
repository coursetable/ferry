import os
from pathlib import Path

import pandas as pd

from ferry.transform.transform_compute import (
    courses_computed,
    evaluation_statistics_computed,
    professors_computed,
    questions_computed,
)
from ferry.transform.import_courses import import_courses
from ferry.transform.import_evaluations import import_evaluations


def transform(data_dir: Path):
    """
    Import the parsed course and evaluation data into CSVs generated with Pandas.

    Used immediately before stage.py as the first step in the import process.
    """

    # get full list of course seasons from files
    (data_dir / "migrated_courses").mkdir(parents=True, exist_ok=True)
    course_seasons = sorted(
        [
            filename.split(".")[0]  # remove the .json extension
            for filename in set(
                os.listdir(data_dir / "migrated_courses")
                + os.listdir(data_dir / "parsed_courses")
            )
            if filename[0] != "."
        ]
    )
    print(f"\nSeason(s): {', '.join(course_seasons)}")
    seasons_table = pd.DataFrame(
        [
            [int(x), {"1": "spring", "2": "summer", "3": "fall"}[x[-1]], int(x[:4])]
            for x in course_seasons
        ],
        columns=["season_code", "term", "year"],
    )

    course_tables = import_courses(
        data_dir / "parsed_courses", data_dir / "migrated_courses", course_seasons
    )
    eval_tables = import_evaluations(
        data_dir / "parsed_evaluations", course_tables["listings"]
    )

    print("\nComputing secondary attributes...")

    eval_tables["evaluation_questions"] = questions_computed(
        eval_tables["evaluation_questions"]
    )

    eval_tables["evaluation_statistics"] = evaluation_statistics_computed(
        evaluation_statistics=eval_tables["evaluation_statistics"],
        evaluation_ratings=eval_tables["evaluation_ratings"],
        evaluation_questions=eval_tables["evaluation_questions"],
    )

    course_tables["courses"] = courses_computed(
        courses=course_tables["courses"],
        listings=course_tables["listings"],
        evaluation_statistics=eval_tables["evaluation_statistics"],
        course_professors=course_tables["course_professors"],
    )

    course_tables["professors"] = professors_computed(
        professors=course_tables["professors"],
        course_professors=course_tables["course_professors"],
        evaluation_statistics=eval_tables["evaluation_statistics"],
    )

    print("\033[F", end="")
    print("Computing secondary attributes... ✔")

    # -----------------------------
    # Output tables to disk as CSVs
    # -----------------------------

    print("\nWriting tables to disk as CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    csvs = {"seasons": seasons_table, **course_tables, **eval_tables}

    for table_name, table in csvs.items():
        table.to_csv(csv_dir / f"{table_name}.csv")

    print("\033[F", end="")
    print("Writing tables to disk as CSVs... ✔")

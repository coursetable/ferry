import os
from pathlib import Path
from typing import cast
import ujson

import pandas as pd

from ferry import database
from ferry.memory_benchmark import memory_benchmark, memory_checkpoint
from .transform_compute import (
    courses_computed,
    evaluation_statistics_computed,
    professors_computed,
    questions_computed,
    narratives_computed,
)
from .import_courses import import_courses
from .import_evaluations import import_evaluations
from .cache_id import save_id_cache
from .invariants import check_invariants


def write_csvs(tables: dict[str, pd.DataFrame], data_dir: Path):
    print("\nWriting tables to disk as CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    for table_name, table in tables.items():
        cast(pd.DataFrame, table).to_csv(csv_dir / f"{table_name}.csv", index=False)

    print("\033[F", end="")
    print("Writing tables to disk as CSVs... ✔")


@memory_benchmark(include_dataframes=True, force_gc=True)
async def transform(data_dir: Path) -> dict[str, pd.DataFrame]:
    """
    Import the parsed course and evaluation data into CSVs generated with Pandas.
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

    with memory_checkpoint("import_courses", include_dataframes=True):
        course_tables = import_courses(data_dir, course_seasons)
    
    with memory_checkpoint("import_evaluations", include_dataframes=True, listings=course_tables["listings"]):
        eval_tables = import_evaluations(data_dir, course_tables["listings"])

    print("\nComputing secondary attributes...")

    with memory_checkpoint("questions_computed"):
        eval_tables["evaluation_questions"] = questions_computed(
            eval_tables["evaluation_questions"]
        )

    with memory_checkpoint("narratives_computed"):
        eval_tables["evaluation_narratives"] = narratives_computed(
            eval_tables["evaluation_narratives"]
        )

    with memory_checkpoint("evaluation_statistics_computed"):
        eval_tables["evaluation_statistics"] = evaluation_statistics_computed(
            evaluation_statistics=eval_tables["evaluation_statistics"],
            evaluation_ratings=eval_tables["evaluation_ratings"],
            evaluation_questions=eval_tables["evaluation_questions"],
        )

    with memory_checkpoint("professors_computed"):
        course_tables["professors"] = professors_computed(
            professors=course_tables["professors"],
            course_professors=course_tables["course_professors"],
            evaluation_statistics=eval_tables["evaluation_statistics"],
        )

    with memory_checkpoint("courses_computed"):
        course_tables["courses"] = courses_computed(
            courses=course_tables["courses"],
            listings=course_tables["listings"],
            evaluation_statistics=eval_tables["evaluation_statistics"],
            course_professors=course_tables["course_professors"],
            professors=course_tables["professors"],
        )
    eval_tables["evaluation_questions"]["options"] = eval_tables[
        "evaluation_questions"
    ]["options"].apply(lambda x: ujson.dumps(x) if isinstance(x, list) else x)

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
        ("course_meetings", database.course_meetings),
        ("locations", database.Location.__table__),
        ("buildings", database.Building.__table__),
        ("evaluation_questions", database.EvaluationQuestion.__table__),
        ("evaluation_narratives", database.EvaluationNarrative.__table__),
        ("evaluation_statistics", database.EvaluationStatistics.__table__),
        ("evaluation_ratings", database.EvaluationRating.__table__),
    ]:

        if table_name == "locations":
            current_table = all_tables[table_name].reset_index(drop=False)
            if "location_id" not in current_table.columns:
                current_table["location_id"] = None
            locations_columns = ["location_id", "building_code", "room"]
            available_cols = [col for col in locations_columns if col in current_table.columns]
            current_table = current_table[available_cols]
            for col in locations_columns:
                if col not in current_table.columns:
                    current_table[col] = None
            all_tables[table_name] = current_table
        elif table_name == "course_meetings":
            # Special handling for course_meetings to preserve temporary location columns
            current_table = all_tables[table_name].reset_index(drop=False)
            db_columns = [column.key for column in db_table.columns]
            
            # Include temporary columns needed for location resolution in sync_db_courses
            temp_columns = ["_building_code", "_room"]
            available_columns = [col for col in db_columns + temp_columns if col in current_table.columns]
            all_tables[table_name] = current_table[available_columns]
        else:
            db_columns = [column.key for column in db_table.columns]
            current_table = all_tables[table_name].reset_index(drop=False)
            
            # Only select columns that exist in both the dataframe and database schema
            available_columns = [col for col in db_columns if col in current_table.columns]
            all_tables[table_name] = current_table[available_columns]

    check_invariants(all_tables)

    print("\033[F", end="")
    print("Computing secondary attributes... ✔")

    save_id_cache(all_tables, data_dir)

    return all_tables

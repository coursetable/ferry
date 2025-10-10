import os
from pathlib import Path
from typing import cast
import ujson

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


async def transform(data_dir: Path, seasons: list[str] | None = None) -> dict[str, pd.DataFrame]:
    """
    Import the parsed course and evaluation data into CSVs generated with Pandas.
    
    Parameters
    ----------
    data_dir : Path
        Directory containing parsed course data
    seasons : list[str] | None
        Optional list of seasons to transform. If None, transforms all seasons found in data_dir.
    """

    # get full list of course seasons from files
    all_course_seasons = sorted(
        [
            filename.split(".")[0]  # remove the .json extension
            for filename in os.listdir(data_dir / "parsed_courses")
            if filename[0] != "."
        ]
    )
    
    # Filter to requested seasons if specified
    if seasons is not None:
        course_seasons = [s for s in all_course_seasons if s in seasons]
        if len(course_seasons) != len(seasons):
            missing = set(seasons) - set(course_seasons)
            if missing:
                print(f"Warning: Seasons {missing} not found in {data_dir / 'parsed_courses'}")
    else:
        course_seasons = all_course_seasons
    
    print(f"\nSeason(s): {', '.join(course_seasons)}")
    seasons_table = pd.DataFrame(
        [
            [x, {"1": "spring", "2": "summer", "3": "fall"}[x[-1]], int(x[:4])]
            for x in course_seasons
        ],
        columns=["season_code", "term", "year"],
    )

    course_tables = import_courses(data_dir, course_seasons)
    eval_tables = import_evaluations(data_dir, course_tables["listings"])

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

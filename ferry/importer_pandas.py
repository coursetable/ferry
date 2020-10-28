import argparse
import contextlib
import os
from pathlib import Path

import numpy as np
import pandas as pd
import ujson
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base

from ferry import config, database
from ferry.config import DATABASE_CONNECT_STRING
from ferry.includes.computed import (
    courses_computed,
    evaluation_statistics_computed,
    professors_computed,
    questions_computed,
)
from ferry.includes.importer import (
    copy_from_stringio,
    import_courses,
    import_demand,
    import_evaluations,
)
from ferry.includes.tqdm import tqdm

"""
================================================================
This script imports the parsed course and evaluation data into the database.
It creates or updates the tables as necessary, so this script is idempotent.
This script does not recalculate any computed values in the schema.
================================================================
"""

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Import classes")
    parser.add_argument(
        "-c", "--cached", help="Load class tables from cached CSVs", action="store_true"
    )
    args = parser.parse_args()

    if not args.cached:

        # ---------------------
        # Get seasons to import
        # ---------------------
        # get full list of course seasons from files
        course_seasons = sorted(
            [
                filename.split(".")[0]  # remove the .json extension
                for filename in set(
                    os.listdir(f"{config.DATA_DIR}/migrated_courses/")
                    + os.listdir(f"{config.DATA_DIR}/parsed_courses/")
                )
                if filename[0] != "."
            ]
        )

        # get full list of demand seasons from files
        demand_seasons = sorted(
            [
                filename.split("_")[0]  # remove the _demand.json suffix
                for filename in os.listdir(f"{config.DATA_DIR}/demand_stats/")
                if filename[0] != "." and filename != "subjects.json"
            ]
        )

        # ----------------------
        # Import course listings
        # ----------------------

        print("\n[Importing courses]")
        print(f"Season(s): {', '.join(course_seasons)}")

        merged_course_info = []

        for season in tqdm(course_seasons, desc="Loading course JSONs"):
            # Read the course listings, giving preference to freshly parsed over migrated ones.
            parsed_courses_file = Path(
                f"{config.DATA_DIR}/parsed_courses/{season}.json"
            )

            if parsed_courses_file.is_file():
                parsed_course_info = pd.read_json(parsed_courses_file)
            else:
                # check migrated courses as a fallback
                migrated_courses_file = Path(
                    f"{config.DATA_DIR}/migrated_courses/{season}.json"
                )

                if not migrated_courses_file.is_file():
                    print(
                        f"Skipping season {season}: not found in parsed or migrated courses."
                    )
                    continue
                with open(migrated_courses_file, "r") as f:
                    parsed_course_info = pd.read_json(migrated_courses_file)

            parsed_course_info["season_code"] = season
            merged_course_info.append(parsed_course_info)

        merged_course_info = pd.concat(merged_course_info, axis=0)
        merged_course_info = merged_course_info.reset_index(drop=True)

        courses, listings, course_professors, professors = import_courses(
            merged_course_info, course_seasons
        )

        # ------------------------
        # Import demand statistics
        # ------------------------

        merged_demand_info = []

        print("\n[Importing demand statistics]")
        print(f"Season(s): {', '.join(demand_seasons)}")
        for season in tqdm(demand_seasons, desc="Loading demand JSONs"):

            demand_file = Path(f"{config.DATA_DIR}/demand_stats/{season}_demand.json")

            if not demand_file.is_file():
                print(f"Skipping season {season}: demand statistics file not found.")
                continue

            with open(demand_file, "r") as f:
                demand_info = pd.read_json(f)

            demand_info["season_code"] = season
            merged_demand_info.append(demand_info)

        merged_demand_info = pd.concat(merged_demand_info, axis=0)
        merged_demand_info = merged_demand_info.reset_index(drop=True)

        demand_statistics = import_demand(merged_demand_info, listings, demand_seasons)

        # -------------------------
        # Import course evaluations
        # -------------------------

        print("\n[Importing course evaluations]")

        # list available evaluation files
        previous_eval_files = Path(config.DATA_DIR / "previous_evals").glob("*.json")
        new_eval_files = Path(config.DATA_DIR / "course_evals").glob("*.json")

        previous_eval_files = [x.name for x in previous_eval_files]
        new_eval_files = [x.name for x in new_eval_files]

        all_evals = sorted(list(set(previous_eval_files + new_eval_files)))

        merged_evaluations_info = []

        for filename in tqdm(all_evals, desc="Loading evaluation JSONs"):
            # Read the evaluation, giving preference to current over previous.
            current_evals_file = Path(f"{config.DATA_DIR}/course_evals/{filename}")

            if current_evals_file.is_file():
                with open(current_evals_file, "r") as f:
                    evaluation = ujson.load(f)
            else:
                with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
                    evaluation = ujson.load(f)

            merged_evaluations_info.append(evaluation)

        merged_evaluations_info = pd.DataFrame(merged_evaluations_info)

        (
            evaluation_narratives,
            evaluation_ratings,
            evaluation_statistics,
            evaluation_questions,
        ) = import_evaluations(merged_evaluations_info, listings)

        # define seasons table for import
        seasons = pd.DataFrame(course_seasons, columns=["season_code"], dtype=int)
        seasons["term"] = seasons["season_code"].apply(
            lambda x: {"1": "spring", "2": "summer", "3": "fall"}[str(x)[-1]]
        )
        seasons["year"] = (
            seasons["season_code"].astype(str).apply(lambda x: x[:4]).astype(int)
        )

        # ----------------------------
        # Compute secondary attributes
        # ----------------------------

        print("\n[Computing secondary attributes]")

        print("Assigning question tags")
        evaluation_questions = questions_computed(evaluation_questions)
        print("Computing average ratings by course")
        evaluation_statistics = evaluation_statistics_computed(
            evaluation_statistics, evaluation_ratings, evaluation_questions
        )
        print("Computing historical ratings for courses")
        courses = courses_computed(courses, listings, evaluation_statistics)
        print("Computing ratings for professors")
        professors = professors_computed(
            professors, course_professors, evaluation_statistics
        )

        # -----------------------------
        # Output tables to disk as CSVs
        # -----------------------------

        print("\n[Writing tables to disk as CSVs]")

        csv_dir = config.DATA_DIR / "importer_dumps"

        seasons.to_csv(csv_dir / "seasons.csv")

        courses.to_csv(csv_dir / "courses.csv")
        listings.to_csv(csv_dir / "listings.csv")
        professors.to_csv(csv_dir / "professors.csv")
        course_professors.to_csv(csv_dir / "course_professors.csv")

        demand_statistics.to_csv(csv_dir / "demand_statistics.csv")

        evaluation_questions.to_csv(csv_dir / "evaluation_questions.csv")
        evaluation_narratives.to_csv(csv_dir / "evaluation_narratives.csv")
        evaluation_ratings.to_csv(csv_dir / "evaluation_ratings.csv")
        evaluation_statistics.to_csv(csv_dir / "evaluation_statistics.csv")

    elif args.cached:

        print("\n[Reading in tables from CSVs]")

        csv_dir = config.DATA_DIR / "importer_dumps"

        seasons = pd.read_csv(csv_dir / "seasons.csv", index_col=0)

        courses = pd.read_csv(csv_dir / "courses.csv", index_col=0)
        listings = pd.read_csv(csv_dir / "listings.csv", index_col=0)
        professors = pd.read_csv(csv_dir / "professors.csv", index_col=0)
        course_professors = pd.read_csv(csv_dir / "course_professors.csv", index_col=0)

        demand_statistics = pd.read_csv(csv_dir / "demand_statistics.csv", index_col=0)

        evaluation_questions = pd.read_csv(
            csv_dir / "evaluation_questions.csv", index_col=0
        )
        evaluation_narratives = pd.read_csv(
            csv_dir / "evaluation_narratives.csv", index_col=0
        )
        evaluation_ratings = pd.read_csv(
            csv_dir / "evaluation_ratings.csv", index_col=0
        )
        evaluation_statistics = pd.read_csv(
            csv_dir / "evaluation_statistics.csv", index_col=0
        )

    # --------------------------
    # Replace tables in database
    # --------------------------
    print("\n[Clearing database]")

    meta = MetaData(bind=database.Engine, reflect=True)
    staging_tables = []

    for table in meta.sorted_tables:
        if not table.name.endswith("_staged"):
            args = []
            for column in table.columns:
                args.append(column.copy())
            for constraint in table.constraints:
                args.append(constraint.copy())
            staging_tables.append(
                Table(
                    f"{table.name}_staged", table.metadata, extend_existing=True, *args
                )
            )

    meta.create_all(tables=staging_tables)

    # drop old tables
    conn = database.Engine.connect()
    delete = conn.begin()
    for table in meta.sorted_tables:
        if table.name.endswith("_staged"):
            conn.execute(f'ALTER TABLE "{table.name}" DISABLE TRIGGER ALL;')
            conn.execute(table.delete())
            conn.execute(f'ALTER TABLE "{table.name}" ENABLE TRIGGER ALL;')
    delete.commit()

    print("\n[Updating database]")

    raw_conn = database.Engine.raw_connection()

    # seasons
    copy_from_stringio(raw_conn, seasons, "seasons_staged")

    # courses
    copy_from_stringio(raw_conn, courses, "courses_staged")
    copy_from_stringio(raw_conn, listings, "listings_staged")
    copy_from_stringio(raw_conn, professors, "professors_staged")
    copy_from_stringio(raw_conn, course_professors, "course_professors_staged")

    # demand statistics
    copy_from_stringio(raw_conn, demand_statistics, "demand_statistics_staged")

    # evaluations
    copy_from_stringio(raw_conn, evaluation_questions, "evaluation_questions_staged")
    copy_from_stringio(raw_conn, evaluation_narratives, "evaluation_narratives_staged")
    copy_from_stringio(raw_conn, evaluation_ratings, "evaluation_ratings_staged")
    copy_from_stringio(raw_conn, evaluation_statistics, "evaluation_statistics_staged")

    print("Committing new tables")
    raw_conn.commit()

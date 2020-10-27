import argparse
import csv
import os
from io import StringIO
from pathlib import Path


import contextlib
import d6tstack
import numpy as np
import pandas as pd
import ujson
import psycopg2
from sqlalchemy import MetaData
from ferry.config import DATABASE_CONNECT_STRING
from sqlalchemy.ext.declarative import declarative_base

from ferry import config, database
from ferry.includes.importer import (
    get_all_tables,
    import_courses,
    import_demand,
    import_evaluations,
)
from ferry.includes.tqdm import tqdm


class DatabaseError(Exception):
    print


"""
================================================================
This script imports the parsed course and evaluation data into the database.
It creates or updates the tables as necessary, so this script is idempotent.
This script does not recalculate any computed values in the schema.
================================================================
"""

if __name__ == "__main__":

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
        parsed_courses_file = Path(f"{config.DATA_DIR}/parsed_courses/{season}.json")

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

    # ------------------------
    # Replace tables in database
    # ------------------------
    print("\n[Clearing database]")

    # drop old tables
    meta = MetaData(bind=database.Engine, reflect=True)
    conn = database.Engine.connect()
    delete = conn.begin()
    for table in meta.sorted_tables:
        conn.execute(f'ALTER TABLE "{table.name}" DISABLE TRIGGER ALL;')
        conn.execute(table.delete())
        conn.execute(f'ALTER TABLE "{table.name}" ENABLE TRIGGER ALL;')
    delete.commit()

    print("\n[Updating database]")

    db_args = {"conn": database.Engine, "if_exists": "append", "index": False}

    seasons = pd.DataFrame(course_seasons, columns=["season_code"], dtype=int)
    seasons["term"] = -1
    seasons["year"] = -1

    def copy_from_stringio(conn, df, table: str):
        """
        Save DataFrame in-memory and migrate
        to database with copy_from().

        Parameters
        ----------
        conn: psycopg2 connection object

        df: DataFrame to import

        table: name of target table

        Returns
        -------
        """

        # create in-memory buffer for DataFrame
        buffer = StringIO()

        df.to_csv(
            buffer,
            index_label="id",
            header=False,
            index=False,
            sep="\t",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            na_rep="NULL",
        )
        buffer.seek(0)

        cursor = conn.cursor()

        try:
            cursor.copy_from(buffer, table, columns=df.columns, sep="\t", null="NULL")
        except (Exception, psycopg2.DatabaseError) as error:
            cursor.rollback()
            cursor.close()
            raise DatabaseError

        print(f"Successfully copied {table}")
        cursor.close()

    conn = psycopg2.connect(
        **{
            "host": "localhost",
            "database": "postgres",
            "user": "postgres",
            "password": "thisisapassword",
        }
    )

    copy_from_stringio(conn, seasons, "seasons")

    copy_from_stringio(conn, courses, "courses")
    copy_from_stringio(conn, listings, "listings")
    copy_from_stringio(conn, professors, "professors")
    copy_from_stringio(conn, course_professors, "course_professors")

    copy_from_stringio(conn, demand_statistics, "demand_statistics")

    copy_from_stringio(conn, evaluation_questions, "evaluation_questions")
    copy_from_stringio(conn, evaluation_narratives, "evaluation_narratives")
    copy_from_stringio(conn, evaluation_ratings, "evaluation_ratings")
    copy_from_stringio(conn, evaluation_statistics, "evaluation_statistics")

    print("Committing new tables")
    conn.commit()

import os
from pathlib import Path

import numpy as np
import pandas as pd
import ujson

from ferry import config
from ferry.config import DATABASE_CONNECT_STRING
from ferry.includes.computed import (
    courses_computed,
    evaluation_statistics_computed,
    professors_computed,
    questions_computed,
)
from ferry.includes.importer import import_courses, import_demand, import_evaluations
from ferry.includes.tqdm import tqdm

"""
================================================================
This script imports the parsed course and evaluation data into
CSVs generated with Pandas.
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

    print("[Importing courses]")
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

    (
        courses,
        listings,
        course_professors,
        professors,
        course_flags,
        flags,
    ) = import_courses(merged_course_info, course_seasons)
    del merged_course_info

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

    merged_evaluations = pd.read_json(f"{config.DATA_DIR}/merged_evaluations.json")

    (
        evaluation_narratives,
        evaluation_ratings,
        evaluation_statistics,
        evaluation_questions,
    ) = import_evaluations(merged_evaluations, listings)

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
    print("Computing courses")
    courses = courses_computed(
        courses, listings, evaluation_statistics, course_professors
    )
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
    flags.to_csv(csv_dir / "flags.csv")
    course_flags.to_csv(csv_dir / "course_flags.csv")

    demand_statistics.to_csv(csv_dir / "demand_statistics.csv")

    evaluation_questions.to_csv(csv_dir / "evaluation_questions.csv")
    evaluation_narratives.to_csv(csv_dir / "evaluation_narratives.csv")
    evaluation_ratings.to_csv(csv_dir / "evaluation_ratings.csv")
    evaluation_statistics.to_csv(csv_dir / "evaluation_statistics.csv")

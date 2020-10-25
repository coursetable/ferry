import argparse
import os
from pathlib import Path

import numpy as np
import pandas as pd
import ujson

from ferry import config, database
from ferry.includes.importer import (
    get_all_tables,
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
    # allow the user to specify seasons (useful for testing and debugging)
    parser = argparse.ArgumentParser(description="Import classes")
    parser.add_argument(
        "-s",
        "--seasons",
        nargs="+",
        help="seasons to import (if empty, import all migrated+parsed courses)",
        default=None,
        required=False,
    )
    parser.add_argument(
        "-m",
        "--mode",
        choices=["courses", "evals", "demand", "all"],
        help="information to import: courses, evals, demand, or all (default)",
        default="all",
        required=False,
    )

    args = parser.parse_args()
    seasons = args.seasons

    # Course information.
    if seasons is None:

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
    else:
        course_seasons = seasons
        demand_seasons = seasons

    # ----------------------
    # Import course listings
    # ----------------------

    # Course listings.
    print("\n[Importing courses]")
    print(f"Season(s): {', '.join(course_seasons)}")

    merged_course_info = []

    for season in course_seasons:
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

    merged_course_info = pd.concat(merged_course_info, axis=0, sort=True)
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
    for season in demand_seasons:

        demand_file = Path(f"{config.DATA_DIR}/demand_stats/{season}_demand.json")

        if not demand_file.is_file():
            print(f"Skipping season {season}: demand statistics file not found.")
            continue

        with open(demand_file, "r") as f:
            demand_info = pd.read_json(f)

        demand_info["season_code"] = season
        merged_demand_info.append(demand_info)

    merged_demand_info = pd.concat(merged_demand_info, axis=0, sort=True)
    merged_demand_info = merged_demand_info.reset_index(drop=True)

    demand_statistics = import_demand(merged_demand_info, listings, seasons)

    # -------------------------
    # Import course evaluations
    # -------------------------

    print("\n[Importing course evaluations]")

    all_evals = [
        filename
        for filename in set(
            os.listdir(f"{config.DATA_DIR}/previous_evals/")
            + os.listdir(f"{config.DATA_DIR}/course_evals/")
        )
        if filename[0] != "."
    ]

    # Filter by seasons.
    if seasons is None:
        evals_to_import = sorted(list(all_evals))

    else:
        evals_to_import = sorted(
            filename for filename in all_evals if filename.split("-")[0] in seasons
        )

    merged_evaluations_info = []

    for filename in tqdm(evals_to_import, desc="Loading evaluations"):
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

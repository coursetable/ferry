import argparse
import os
from pathlib import Path

import textdistance
import ujson

from ferry import config, database
from ferry.includes.tqdm import tqdm
from ferry.includes.importer import get_all_tables

"""
================================================================
This script imports the parsed course and evaluation data into the database.
It creates or updates the tables as necessary, so this script is idempotent.
This script does not recalculate any computed values in the schema.
================================================================
"""


# def import_courses(tables, parsed_course_info):


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

    # load all tables into Pandas
    tables = get_all_tables(["public"])

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

    # Course listings.
    if args.mode == "courses" or args.mode == "all":
        print(f"Importing courses for season(s): {course_seasons}")
        for season in course_seasons:
            # Read the course listings, giving preference to freshly parsed over migrated ones.
            parsed_courses_file = Path(
                f"{config.DATA_DIR}/parsed_courses/{season}.json"
            )

            if parsed_courses_file.is_file():
                with open(parsed_courses_file, "r") as f:
                    parsed_course_info = ujson.load(f)
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
                    parsed_course_info = ujson.load(f)

            import_courses(tables, parsed_course_info)

            #     with database.session_scope(database.Session) as session:
            #         # tqdm.write(f"Importing {course_info}")
            #         import_course(session, course_info)

    # # Course demand.
    # if args.mode == "demand" or args.mode == "all":
    #     # Compute seasons.

    #     print(f"Importing demand stats for seasons: {demand_seasons}")
    #     for season in demand_seasons:

    #         demand_file = Path(f"{config.DATA_DIR}/demand_stats/{season}_demand.json")

    #         if not demand_file.is_file():
    #             print(f"Skipping season {season}: demand statistics file not found.")
    #             continue

    #         with open(demand_file, "r") as f:
    #             demand_stats = ujson.load(f)

    #         for demand_info in tqdm(
    #             demand_stats, desc=f"Importing demand stats for {season}"
    #         ):
    #             with database.session_scope(database.Session) as session:
    #                 import_demand(session, season, demand_info)

    # # Course evaluations.
    # if args.mode == "evals" or args.mode == "all":
    #     all_evals = [
    #         filename
    #         for filename in set(
    #             os.listdir(f"{config.DATA_DIR}/previous_evals/")
    #             + os.listdir(f"{config.DATA_DIR}/course_evals/")
    #         )
    #         if filename[0] != "."
    #     ]

    #     # Filter by seasons.
    #     if seasons is None:
    #         evals_to_import = sorted(list(all_evals))

    #     else:
    #         evals_to_import = sorted(
    #             filename for filename in all_evals if filename.split("-")[0] in seasons
    #         )

    #     for filename in tqdm(evals_to_import, desc="Importing evaluations"):
    #         # Read the evaluation, giving preference to current over previous.
    #         current_evals_file = Path(f"{config.DATA_DIR}/course_evals/{filename}")

    #         if current_evals_file.is_file():
    #             with open(current_evals_file, "r") as f:
    #                 evaluation = ujson.load(f)
    #         else:
    #             with open(f"{config.DATA_DIR}/previous_evals/{filename}", "r") as f:
    #                 evaluation = ujson.load(f)

    #         with database.session_scope(database.Session) as session:
    #             # tqdm.write(f"Importing evaluation {evaluation}")
    #             import_evaluation(session, evaluation)

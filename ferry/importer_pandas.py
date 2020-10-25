import argparse
import os
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import textdistance
import ujson

from ferry import config, database
from ferry.includes.importer import get_all_tables
from ferry.includes.tqdm import tqdm
from ferry.includes.utils import invert_dict_of_lists, merge_overlapping

"""
================================================================
This script imports the parsed course and evaluation data into the database.
It creates or updates the tables as necessary, so this script is idempotent.
This script does not recalculate any computed values in the schema.
================================================================
"""


def import_courses(merged_course_info, seasons):

    # seasons must be sorted in ascending order

    print("Aggregating cross-listings")
    merged_course_info["season_code"] = merged_course_info["season_code"].astype(int)
    merged_course_info["crn"] = merged_course_info["crn"].astype(int)
    merged_course_info["crns"] = merged_course_info["crns"].apply(lambda x: map(int, x))

    # group CRNs by season for cross-listing deduplication
    crns_by_season = merged_course_info.groupby("season_code")["crns"].apply(list)
    # convert CRN groups to sets for resolution
    crns_by_season = crns_by_season.apply(lambda x: [set(y) for y in x])
    # resolve overlapping CRN sets
    crn_groups_by_season = crns_by_season.apply(merge_overlapping)

    print("Mapping out cross-listings")
    # map CRN groups to IDs
    temp_course_ids_by_season = crns_by_season.apply(
        lambda x: invert_dict_of_lists(dict(enumerate(x)))
    )
    temp_course_ids_by_season = temp_course_ids_by_season.to_dict()

    # assign season-specific ID based on CRN group IDs
    merged_course_info["season_course_id"] = merged_course_info.apply(
        lambda row: temp_course_ids_by_season[row["season_code"]][row["crn"]], axis=1
    )
    # temporary string-based unique course identifier
    merged_course_info["temp_course_id"] = merged_course_info.apply(
        lambda x: f"{x['season_code']}_{x['season_course_id']}", axis=1
    )

    print("Creating courses table")
    # initialize courses table
    courses = merged_course_info.drop_duplicates(subset="temp_course_id").copy(
        deep=True
    )
    courses["course_id"] = range(len(courses))

    print("Creating listings table")
    temp_to_course_id = dict(zip(courses["temp_course_id"], courses["course_id"]))

    # initialize listings table
    listings = merged_course_info.copy(deep=True)
    listings["listing_id"] = range(len(listings))
    listings["course_id"] = listings["temp_course_id"].apply(temp_to_course_id.get)

    print("Creating professors table")
    # initialize professors table
    professors_prep = courses.loc[
        :,
        ["season_code", "course_id", "professors", "professor_emails", "professor_ids"],
    ]

    print("Resolving professor attributes")
    professors_prep["professors"] = professors_prep["professors"].apply(
        lambda x: x if x == x else []
    )
    professors_prep["professor_emails"] = professors_prep["professor_emails"].apply(
        lambda x: x if x == x else []
    )
    professors_prep["professor_ids"] = professors_prep["professor_ids"].apply(
        lambda x: x if x == x else []
    )

    professors_prep["professors_info"] = professors_prep[
        ["professors", "professor_emails", "professor_ids"]
    ].to_dict(orient="split")["data"]

    def zip_professors_info(professors_info):

        names, emails, ocs_ids = professors_info

        names = list(filter(lambda x: x != "", names))
        emails = list(filter(lambda x: x != "", emails))
        ocs_ids = list(filter(lambda x: x != "", ocs_ids))

        if len(names) == 0:
            return []

        # account for inconsistent lengths before zipping
        if len(emails) != len(names):
            emails = [None] * len(names)
        if len(ocs_ids) != len(names):
            ocs_ids = [None] * len(names)

        return list(zip(names, emails, ocs_ids))

    professors_prep["professors_info"] = professors_prep["professors_info"].apply(
        zip_professors_info
    )

    professors_prep = professors_prep[professors_prep["professors_info"].apply(len) > 0]

    # expand courses with multiple professors
    professors_prep = professors_prep.loc[
        :, ["season_code", "course_id", "professors_info"]
    ].explode("professors_info")
    professors_prep = professors_prep.reset_index(drop=True)

    # expand professor info columns
    professors_prep[["name", "email", "ocs_id"]] = pd.DataFrame(
        professors_prep["professors_info"].tolist(), index=professors_prep.index
    )

    print("Constructing professors table in chronological order")
    professors = pd.DataFrame(columns=["professor_id", "name", "email", "ocs_id"])

    professors_by_season = professors_prep.groupby("season_code")

    def get_professor_identifiers(professors):

        names_ids = (
            professors.dropna(subset=["name"])
            .groupby("name")["professor_id"]
            .apply(list)
            .to_dict()
        )
        emails_ids = (
            professors.dropna(subset=["email"])
            .groupby("email")["professor_id"]
            .apply(list)
            .to_dict()
        )
        ocs_ids = (
            professors.dropna(subset=["ocs_id"])
            .groupby("ocs_id")["professor_id"]
            .apply(list)
            .to_dict()
        )

        return names_ids, emails_ids, ocs_ids

    def match_professors(season_professors, professors):

        names_ids, emails_ids, ocs_ids = get_professor_identifiers(professors)

        # get ID matches by field
        season_professors["name_matched_ids"] = season_professors["name"].apply(
            lambda x: names_ids.get(x, [])
        )
        season_professors["email_matched_ids"] = season_professors["email"].apply(
            lambda x: emails_ids.get(x, [])
        )
        season_professors["ocs_matched_ids"] = season_professors["ocs_id"].apply(
            lambda x: ocs_ids.get(x, [])
        )

        season_professors["matched_ids_aggregate"] = (
            season_professors["name_matched_ids"]
            + season_professors["email_matched_ids"]
            + season_professors["ocs_matched_ids"]
        )

        season_professors["matched_ids_aggregate"] = season_professors[
            "matched_ids_aggregate"
        ].apply(lambda x: x if len(x) > 0 else [None])

        professor_ids = season_professors["matched_ids_aggregate"].apply(
            lambda x: Counter(x).most_common(1)[0][0]
        )

        return professor_ids

    course_professors = []

    # build professors table in order of seasons
    for season in seasons:

        season_professors = professors_by_season.get_group(int(season)).copy(deep=True)

        # first-pass
        season_professors["professor_id"] = match_professors(
            season_professors, professors
        )

        professors_update = season_professors.drop_duplicates("professors_info").copy(
            deep=True
        )
        new_professors = professors_update[professors_update["professor_id"].isna()]

        max_professor_id = max(list(professors["professor_id"]) + [0])
        new_professor_ids = pd.Series(
            range(max_professor_id + 1, max_professor_id + len(new_professors) + 1),
            index=new_professors.index,
            dtype=np.float64,
        )
        professors_update["professor_id"].update(new_professor_ids)

        professors = professors_update[professors.columns].combine_first(professors)
        professors["professor_id"] = professors["professor_id"].astype(int)

        # second-pass
        season_professors["professor_id"] = match_professors(
            season_professors, professors
        )

        course_professors.append(season_professors[["course_id", "professor_id"]])

    course_professors = pd.concat(course_professors, axis=0, sort=True)

    return courses, listings, course_professors, professors


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

    # Course listings.
    if args.mode == "courses" or args.mode == "all":
        print(f"Importing courses for season(s): {course_seasons}")

        merged_course_info = []

        for season in course_seasons:
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

        merged_course_info = pd.concat(merged_course_info, axis=0, sort=True)
        merged_course_info = merged_course_info.reset_index(drop=True)

    courses, listings, course_professors, professors = import_courses(
        merged_course_info, course_seasons
    )

    print(f"Total courses: {len(courses)}")
    print(f"Total listings: {len(listings)}")
    print(f"Total course-professors: {len(course_professors)}")
    print(f"Total professors: {len(professors)}")

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

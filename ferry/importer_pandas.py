import argparse
import os
from collections import Counter
from pathlib import Path

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


def import_courses(tables, parsed_course_info, season):

    # get cross-listing groups
    cross_listing_groups = merge_overlapping(parsed_course_info["crns"].apply(set))
    # set temporary course IDs for cross-listing deduplication
    crn_to_temp_id = invert_dict_of_lists(dict(enumerate(cross_listing_groups)))
    parsed_course_info["temp_course_id"] = (
        parsed_course_info["crn"].astype(str).apply(crn_to_temp_id.get)
    )
    # specify season
    parsed_course_info["season_code"] = season

    listings_update = parsed_course_info.loc[
        :, ["subject", "number", "section", "crn", "season_code"]
    ]
    listings_update["course_code"] = (
        listings_update["subject"] + " " + listings_update["number"]
    )
    listings_update = listings_update.set_index("crn", drop=False)

    # extract old listings for current season
    old_listings = tables["listings"].copy(deep=True)
    old_listings = old_listings[old_listings["season_code"] == season]
    old_listings = old_listings.set_index("crn", drop=False)

    # combine listings (priority given to new values)
    listings = listings_update.combine_first(old_listings)

    # add new listing IDs based on old ones
    max_listing_id = max(tables["listings"]["listing_id"])
    needs_listing_ids = listings.index[listings["listing_id"].isna()]
    new_listing_ids = pd.Series(
        range(max_listing_id + 1, max_listing_id + len(needs_listing_ids) + 1),
        index=needs_listing_ids.index,
    )
    listings["listing_id"].update(new_listing_ids)
    listings["listing_id"] = listings["listing_id"].astype(int)
    listings = listings.reset_index(drop=True)

    # create new courses
    # we add a new course whenever any of its CRNs is not in listings yet
    # so we want the CRN groups that do not intersect with the CRNs in listings
    old_listing_crns = set(old_listings["crn"])
    new_crn_groups = [x for x in cross_listing_groups if len(x & old_listing_crns) == 0]
    # since cross-listings should be the same courses, just take the first ones
    new_crn_groups = [sorted(list(x))[0] for x in new_crn_groups]

    old_courses = tables["courses"].copy(deep=True)
    old_courses = old_courses[old_courses["season_code"] == season]

    crn_to_course_id = dict(zip(listings["crn"], listings["course_id"]))
    parsed_course_info["course_id"] = parsed_course_info["crn"].apply(
        crn_to_course_id.get
    )

    # remove cross-listed courses (prefer ones with existing course ID)
    parsed_course_info = parsed_course_info.sort_values(
        by="course_id", na_position="last"
    )
    courses = parsed_course_info.drop_duplicates(
        subset="temp_course_id", keep="first"
    ).copy(deep=True)

    # assign new course IDs
    max_course_id = max(tables["courses"]["course_id"])
    needs_course_ids = courses.index[courses["course_id"].isna()]
    new_course_ids = pd.Series(
        range(max_course_id + 1, max_course_id + len(needs_course_ids) + 1),
        index=needs_course_ids,
    )
    courses["course_id"].update(new_course_ids)
    courses["course_id"] = courses["course_id"].astype(int)

    # update course_ids in listings
    temp_id_to_course_id = dict(zip(courses["temp_course_id"], courses["course_id"]))
    new_crn_to_course_id = {
        int(crn): temp_id_to_course_id[temp_id]
        for crn, temp_id in crn_to_temp_id.items()
    }
    listings["course_id"] = listings["crn"].apply(new_crn_to_course_id.get)

    # update professors
    courses["professor_infos"] = courses.apply(
        lambda x: list(zip(x["professors"], x["professor_emails"], x["professor_ids"])),
        axis=1,
    )

    # initialize courses-to-professors
    courses_with_professors = courses[courses["professor_infos"].apply(len) > 0]
    courses_professors = courses_with_professors.loc[
        :, ["course_id", "professor_infos"]
    ].explode("professor_infos")

    # expand professor info tuples to own columns
    courses_professors[["name", "email", "ocs_id"]] = pd.DataFrame(
        courses_professors["professor_infos"].tolist(), index=courses_professors.index
    )

    # get professors only
    professors_update = courses_professors[["name", "email", "ocs_id"]]
    # assume OCS ID is unique professor identifier within a season
    professors_update = professors_update.drop_duplicates("ocs_id")

    old_professors = tables["professors"].copy(deep=True)
    names_ids = old_professors.groupby("name")["professor_id"].apply(list).to_dict()
    emails_ids = old_professors.groupby("email")["professor_id"].apply(list).to_dict()
    ocs_ids = old_professors.groupby("ocs_id")["professor_id"].apply(list).to_dict()

    professors_update["name_matched_ids"] = professors_update["name"].apply(
        lambda x: names_ids.get(x, [])
    )
    professors_update["email_matched_ids"] = professors_update["email"].apply(
        lambda x: emails_ids.get(x, [])
    )
    professors_update["ocs_matched_ids"] = professors_update["ocs_id"].apply(
        lambda x: ocs_ids.get(x, [])
    )

    professors_update["matched_ids_aggregate"] = (
        professors_update["name_matched_ids"]
        + professors_update["email_matched_ids"]
        + professors_update["ocs_matched_ids"]
    )

    professors_update["matched_ids_aggregate"] = professors_update[
        "matched_ids_aggregate"
    ].apply(lambda x: x if x == x else [None])

    professors_update["matched_id"] = professors_update["matched_ids_aggregate"].apply(
        lambda x: Counter(x).most_common(1)[0][0]
    )


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
                    parsed_course_info = pd.read_json(parsed_courses_file)

            import_courses(tables, parsed_course_info, season)

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

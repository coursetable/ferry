import argparse
import os
from collections import Counter
from itertools import combinations
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
        ].apply(lambda x: x if len(x) > 0 else [np.nan])

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

        professors_update = season_professors.drop_duplicates(
            subset=["name", "email", "ocs_id"], keep="first"
        ).copy(deep=True)

        new_professors = professors_update[professors_update["professor_id"].isna()]

        max_professor_id = max(list(professors["professor_id"]) + [0])
        new_professor_ids = pd.Series(
            range(max_professor_id + 1, max_professor_id + len(new_professors) + 1),
            index=new_professors.index,
            dtype=np.float64,
        )
        professors_update["professor_id"].update(new_professor_ids)
        professors_update["professor_id"] = professors_update["professor_id"].astype(
            int
        )
        professors_update = professors_update.drop_duplicates(
            subset=["professor_id"], keep="first"
        )
        professors_update = professors_update.set_index("professor_id")

        professors = professors.set_index("professor_id", drop=True)
        professors = professors_update[professors.columns].combine_first(professors)
        professors = professors.reset_index(drop=False)

        # second-pass
        season_professors["professor_id"] = match_professors(
            season_professors, professors
        )

        course_professors.append(season_professors[["course_id", "professor_id"]])

    course_professors = pd.concat(course_professors, axis=0, sort=True)

    return courses, listings, course_professors, professors


def import_demand(merged_demand_info, listings, seasons):

    demand_statistics = merged_demand_info.copy(deep=True)

    # construct outer season grouping
    season_code_to_course_id = listings[
        ["season_code", "course_code", "course_id"]
    ].groupby("season_code")

    # construct inner course_code to course_id mapping
    season_code_to_course_id = season_code_to_course_id.apply(
        lambda x: x[["course_code", "course_id"]]
        .groupby("course_code")["course_id"]
        .apply(list)
        .to_dict()
    )

    # cast outer season mapping to dictionary
    season_code_to_course_id = season_code_to_course_id.to_dict()

    def match_demand_to_courses(row):
        season_code = int(row["season_code"])

        course_ids = [
            season_code_to_course_id.get(season_code, {}).get(x, None)
            for x in row["codes"]
        ]

        course_ids = [set(x) for x in course_ids if x is not None]

        if course_ids == []:
            return []

        # union all course IDs
        course_ids = set.union(*course_ids)
        course_ids = sorted(list(course_ids))

        return course_ids

    demand_statistics["course_id"] = demand_statistics.apply(
        match_demand_to_courses, axis=1
    )

    demand_statistics = demand_statistics.loc[
        demand_statistics["course_id"].apply(len) > 0, :
    ]

    def date_to_int(date):
        month, day = date.split("/")

        month = int(month)
        day = int(day)

        return month * 100 + day

    def get_most_recent_demand(row):

        sorted_demand = list(row["overall_demand"].items())
        sorted_demand.sort(key=lambda x: date_to_int(x[0]))
        latest_demand_date, latest_demand = sorted_demand[-1]

        return [latest_demand, latest_demand_date]

    # get most recent demand date
    latest = demand_statistics.apply(get_most_recent_demand, axis=1)
    demand_statistics[["latest_demand", "latest_demand_date"]] = pd.DataFrame(
        latest.values.tolist()
    )

    # expand course_id list to one per row
    demand_statistics = demand_statistics.explode("course_id")

    # rename demand JSON column to match database
    demand_statistics = demand_statistics.rename(
        {"overall_demand": "demand"}, axis="columns"
    )

    # return columns of interest
    demand_statistics = demand_statistics.loc[
        :, ["course_id", "latest_demand", "latest_demand_date", "demand"]
    ]

    return demand_statistics


def import_evaluations(merged_evaluations_info, listings):

    evaluations = merged_evaluations_info.copy(deep=True)

    # construct outer season grouping
    season_crn_to_course_id = listings[["season_code", "course_id", "crn"]].groupby(
        "season_code"
    )

    # construct inner course_code to course_id mapping
    season_crn_to_course_id = season_crn_to_course_id.apply(
        lambda x: x[["crn", "course_id"]].set_index("crn")["course_id"].to_dict()
    )

    # cast outer season mapping to dictionary
    season_crn_to_course_id = season_crn_to_course_id.to_dict()

    # convert evaluation season and crn types for matching
    evaluations["season"] = evaluations["season"].astype(int)
    evaluations["crn_code"] = evaluations["crn_code"].astype(int)

    # find course codes for evaluations
    evaluations["course_id"] = evaluations.apply(
        lambda row: season_crn_to_course_id.get(row["season"], {}).get(
            row["crn_code"], None
        ),
        axis=1,
    )

    # report number of evaluations with missing course codes
    nan_total = evaluations["course_id"].isna().sum()
    print(f"Removing {nan_total}/{len(evaluations)} evaluated courses without matches")

    # remove evaluations with missing course codes
    evaluations = evaluations.dropna(subset=["course_id"], axis=0)
    evaluations["course_id"] = evaluations["course_id"].astype(int)

    evaluation_questions = []

    # extract evaluation narratives
    evaluation_narratives = evaluations[evaluations["narratives"].apply(len) > 0].copy(
        deep=True
    )

    evaluation_narratives = evaluation_narratives.loc[
        :, ["course_id", "narratives", "season"]
    ]

    # expand each question into own column
    evaluation_narratives = evaluation_narratives.explode("narratives")
    evaluation_narratives["question_code"] = evaluation_narratives["narratives"].apply(
        lambda x: x["question_id"]
    )
    evaluation_narratives["question_text"] = evaluation_narratives["narratives"].apply(
        lambda x: x["question_text"]
    )
    evaluation_narratives["comment"] = evaluation_narratives["narratives"].apply(
        lambda x: x["comments"]
    )

    evaluation_narratives["is_narrative"] = True
    evaluation_questions.append(
        evaluation_narratives.loc[
            :, ["season", "question_code", "is_narrative", "question_text"]
        ].copy(deep=True)
    )

    # subset and explode
    evaluation_narratives = evaluation_narratives.loc[
        :, ["course_id", "question_code", "comment"]
    ]
    evaluation_narratives = evaluation_narratives.explode("comment")
    evaluation_narratives = evaluation_narratives.reset_index(drop=True)

    # extract evaluation ratings
    evaluation_ratings = evaluations[evaluations["ratings"].apply(len) > 0].copy(
        deep=True
    )

    evaluation_ratings = evaluation_ratings.loc[:, ["course_id", "ratings", "season"]]
    evaluation_ratings = evaluation_ratings.explode("ratings")

    evaluation_ratings["question_code"] = evaluation_ratings["ratings"].apply(
        lambda x: x["question_id"]
    )
    evaluation_ratings["question_text"] = evaluation_ratings["ratings"].apply(
        lambda x: x["question_text"]
    )
    evaluation_ratings["options"] = evaluation_ratings["ratings"].apply(
        lambda x: x["options"]
    )
    evaluation_ratings["rating"] = evaluation_ratings["ratings"].apply(
        lambda x: x["data"]
    )

    evaluation_ratings["is_narrative"] = False
    evaluation_questions.append(
        evaluation_ratings.loc[
            :, ["question_code", "is_narrative", "question_text", "options", "season"]
        ].copy(deep=True)
    )

    # extract evaluation statistics
    evaluation_statistics = evaluations.loc[
        :, ["course_id", "enrollment", "extras"]
    ].copy(deep=True)
    evaluation_statistics["enrolled"] = evaluation_statistics["enrollment"].apply(
        lambda x: x["enrolled"]
    )
    evaluation_statistics["responses"] = evaluation_statistics["enrollment"].apply(
        lambda x: x["responses"]
    )
    evaluation_statistics["declined"] = evaluation_statistics["enrollment"].apply(
        lambda x: x["declined"]
    )
    evaluation_statistics["no_response"] = evaluation_statistics["enrollment"].apply(
        lambda x: x["no response"]
    )

    evaluation_statistics = evaluation_statistics.loc[
        :, ["course_id", "enrolled", "responses", "declined", "no_response", "extras"]
    ]

    evaluation_questions = pd.concat(evaluation_questions, axis=0, sort=True)
    evaluation_questions = evaluation_questions.reset_index(drop=True)

    # consistency checks
    print("Checking question text consistency")
    text_by_code = evaluation_questions.groupby("question_code")["question_text"].apply(
        set
    )

    # focus on question texts with multiple variations
    text_by_code = text_by_code[text_by_code.apply(len) > 1]
    max_diff_texts = max(text_by_code.apply(len))
    print(f"Maximum number of different texts per question code: {max_diff_texts}")

    def min_pairwise_distance(texts):

        pairs = combinations(texts, 2)
        distances = [textdistance.levenshtein.distance(*pair) for pair in pairs]

        return max(distances)

    distances_by_code = text_by_code.apply(min_pairwise_distance)
    max_all_distances = max(distances_by_code)

    print(f"Maximum text divergence within codes: {max_all_distances}")

    if not all(distances_by_code < 32):

        inconsistent_codes = distances_by_code[distances_by_code >= 32]
        inconsistent_codes = list(inconsistent_codes.index)
        inconsistent_codes = ", ".join(inconsistent_codes)

        raise database.InvariantError(
            f"Error: question codes {inconsistent_codes} have divergent texts"
        )

    print("Checking question type (narrative/rating) consistency")
    is_narrative_by_code = evaluation_questions.groupby("question_code")[
        "is_narrative"
    ].apply(set)

    if not all(is_narrative_by_code.apply(len) == 1):
        inconsistent_codes = is_narrative_by_code[is_narrative_by_code.apply(len) != 1]
        inconsistent_codes = list(inconsistent_codes.index)
        inconsistent_codes = ", ".join(inconsistent_codes)
        raise database.InvariantError(
            f"Error: question codes {inconsistent_codes} have both narratives and ratings"
        )

    # deduplicate questions and keep most recent
    evaluation_questions = evaluation_questions.sort_values(
        by="season", ascending=False
    )
    evaluation_questions = evaluation_questions.drop_duplicates(
        subset=["question_code"], keep="first"
    )

    print(f"Total evaluation narratives: {len(evaluation_narratives)}")
    print(f"Total evaluation ratings: {len(evaluation_ratings)}")
    print(f"Total evaluation statistics: {len(evaluation_statistics)}")
    print(f"Total evaluation questions: {len(evaluation_questions)}")

    return (
        evaluation_narratives,
        evaluation_ratings,
        evaluation_statistics,
        evaluation_questions,
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
    print(f"Importing courses for season(s): {', '.join(course_seasons)}")

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

    print(f"Total courses: {len(courses)}")
    print(f"Total listings: {len(listings)}")
    print(f"Total course-professors: {len(course_professors)}")
    print(f"Total professors: {len(professors)}")

    # Course demand.

    merged_demand_info = []

    print(f"Importing demand stats for seasons: {', '.join(demand_seasons)}")
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

    print(f"Total demand statistics: {len(demand_statistics)}")

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

    for filename in tqdm(evals_to_import, desc="Importing evaluations"):
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

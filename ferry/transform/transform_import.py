"""
Functions for importing information into tables.

Used by /ferry/transform.py.
"""

import logging
from pathlib import Path
from collections import Counter
from itertools import combinations

import numpy as np
import pandas as pd
import textdistance
import ujson
from typing import cast

from ferry import database
from ferry.utils import (
    get_table_columns,
    invert_dict_of_lists,
    merge_overlapping,
)

# maximum question divergence to allow
QUESTION_DIVERGENCE_CUTOFF = 32


# extraneous texts to remove
REMOVE_TEXTS = [
    "(Your anonymous response to this question may be viewed by Yale College students, faculty, and advisers to aid in course selection and evaluating teaching.)"  # pylint: disable=line-too-long
]


def resolve_cross_listings(merged_course_info: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve course cross-listings by computing unique course_ids.

    Parameters
    ----------
    merged_course_info:
        Raw course information from JSON files.

    Returns
    -------
    merged_course_info with 'temp_course_id' field added.
    """

    # seasons must be sorted in ascending order
    # prioritize Yale College courses when deduplicating listings
    logging.debug("Sorting by season and if-undergrad")

    def classify_yc(row: pd.Series):
        if row["school"] == "YC":
            return True

        if row["school"] != row["school"]:
            # check number of numbers in course number
            # (some courses have letters in them)
            num_nums = len([x for x in row["number"] if x.isnumeric()])
            # if the course number is in the 000s to 400s range it's undergrad
            if row["number"][0] in ["0", "1", "2", "3", "4"] and num_nums < 4:
                return True
        return False

    merged_course_info["is_yc"] = merged_course_info.apply(classify_yc, axis=1)
    merged_course_info = merged_course_info.sort_values(
        by=["season_code", "is_yc"], ascending=[True, False]
    )

    logging.debug("Aggregating cross-listings")
    merged_course_info["season_code"] = merged_course_info["season_code"].astype(int)
    merged_course_info["crn"] = merged_course_info["crn"].astype(int)
    merged_course_info["crns"] = merged_course_info["crns"].apply(
        lambda crns: [int(crn) for crn in crns]
    )

    # group CRNs by season for cross-listing deduplication

    crns_by_season = merged_course_info.groupby("season_code")["crns"].apply(list)
    # convert CRN groups to sets for resolution
    crns_by_season = crns_by_season.apply(lambda x: [frozenset(y) for y in x])
    # resolve overlapping CRN sets
    crns_by_season = crns_by_season.apply(merge_overlapping)

    logging.debug("Mapping out cross-listings")
    # map CRN groups to temporary IDs within each season
    temp_course_ids_by_season = crns_by_season.apply(
        lambda x: invert_dict_of_lists(dict(enumerate(x)))
    )
    temp_course_ids_by_season = temp_course_ids_by_season.to_dict()

    # assign season-specific ID based on CRN group IDs
    merged_course_info["season_course_id"] = merged_course_info.apply(
        lambda row: temp_course_ids_by_season[row["season_code"]][row["crn"]],
        axis=1,
    )
    # temporary string-based unique course identifier
    merged_course_info["temp_course_id"] = merged_course_info.apply(
        lambda x: f"{x['season_code']}_{x['season_course_id']}", axis=1
    )

    return merged_course_info


def aggregate_professors(courses: pd.DataFrame) -> pd.DataFrame:
    """
    Aggregate professor info columns in preparation for matching.

    Parameters
    ----------
    courses:
        intermediate courses table from import_courses.

    Returns
    -------
    professors_prep: professor attributes DataFrame
    """
    logging.debug("Aggregating professor attributes")
    # initialize professors table
    professors_prep = courses.loc[
        :,
        [
            "season_code",
            "course_id",
            "professors",
            "professor_emails",
            "professor_ids",
        ],
    ]

    logging.debug("Resolving professor attributes")
    # set default empty value for exploding later on
    professors_prep["professors"] = professors_prep["professors"].apply(
        lambda x: [] if not isinstance(x, list) else x
    )
    professors_prep["professor_emails"] = professors_prep["professor_emails"].apply(
        lambda x: [] if not isinstance(x, list) else x
    )
    professors_prep["professor_ids"] = professors_prep["professor_ids"].apply(
        lambda x: [] if not isinstance(x, list) else x
    )

    # reshape professor attributes array
    all_professors_info: list[list[tuple[str, str | None]]] = []

    for i, row in professors_prep.iterrows():
        names, emails = row["professors"], row["professor_emails"]

        names: list[str] = list(filter(lambda x: x != "", names))
        emails: list[str | None] = list(filter(lambda x: x != "", emails))

        # if no names, return empty regardless of others
        # (professors need to be named)
        if len(names) == 0:
            all_professors_info.append([])
            continue

        # account for inconsistent lengths before zipping
        if len(emails) != len(names):
            emails = [None] * len(names)

        all_professors_info.append(list(zip(names, emails)))

    professors_prep["professors_info"] = all_professors_info

    # exclude instances with empty/bad professor infos
    professors_prep = professors_prep[professors_prep["professors_info"].apply(len) > 0]

    # expand courses with multiple professors
    professors_prep = professors_prep.loc[
        :, ["season_code", "course_id", "professors_info"]
    ].explode("professors_info")
    professors_prep = professors_prep.reset_index(drop=True)

    # expand professor info columns
    professors_prep[["name", "email"]] = pd.DataFrame(
        professors_prep["professors_info"].tolist(), index=professors_prep.index
    )

    return professors_prep


def resolve_professors(
    professors_prep: pd.DataFrame, seasons: list[str]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resolve course-professor mappings and professors table

    Parameters
    ----------
    professors_prep:
        Professor attributes from aggregate_professors.
    seasons:
        List of seasons for sorting purposes.

    Returns
    -------
    professors, course_professors
    """

    logging.debug("Constructing professors table in chronological order")

    professors = pd.DataFrame(columns=["professor_id", "name", "email"])

    professors_by_season = professors_prep.groupby("season_code")

    def get_professor_identifiers(professors):
        # return dictionaries mapping professors to
        # professor_id primary keys by names, emails

        names_ids = (
            professors.dropna(subset=["name"])
            .groupby("name", group_keys=True)["professor_id"]
            .apply(list)
            .to_dict()
        )
        emails_ids = (
            professors.dropna(subset=["email"])
            .groupby("email", group_keys=True)["professor_id"]
            .apply(list)
            .to_dict()
        )

        return names_ids, emails_ids

    def match_professors(
        season_professors: pd.DataFrame, professors: pd.DataFrame
    ) -> pd.Series:
        """
        Match professors within a season to main professors.

        Parameters
        ----------
        season_professors:
            Professors and attributes for a given season
        professors:
            Main professors table to pull attributes from.
        """
        names_ids, emails_ids = get_professor_identifiers(professors)

        # get ID matches by field
        season_professors["name_matched_ids"] = season_professors["name"].apply(
            lambda x: names_ids.get(x, [])
        )
        season_professors["email_matched_ids"] = season_professors["email"].apply(
            lambda x: emails_ids.get(x, [])
        )
        # NOTE: at one point we also used the ocs_id field to do matching. However, it turns out
        # that Yale recycles OCS IDs, so we can't use it without a bunch of wrong matches.

        # aggregate found IDs
        season_professors["matched_ids_aggregate"] = (
            season_professors["name_matched_ids"]
            + season_professors["email_matched_ids"]
        )

        # aggregate ID matches
        season_professors["matched_ids_aggregate"] = season_professors[
            "matched_ids_aggregate"
        ].apply(lambda x: x if len(x) > 0 else [np.nan])

        # use the most-common matched ID
        professor_ids = season_professors["matched_ids_aggregate"].apply(
            lambda x: Counter(x).most_common(1)[0][0]
        )

        ties = season_professors["matched_ids_aggregate"].apply(
            lambda x: Counter(x).most_common(2)
        )
        ties = ties.apply(lambda x: False if len(x) != 2 else x[0][1] == x[1][1])

        for i, row in season_professors[ties].iterrows():
            logging.debug(
                f"Professor {row['name']} ({row['email']}) has tied matches: { sorted(list(set(row['matched_ids_aggregate']))) }",
            )

        return professor_ids

    # course-professors junction table
    # store as list of DataFrames before concatenation
    course_professors_ = []

    # build professors table in order of seasons
    for season in seasons:

        season_professors = professors_by_season.get_group(int(season)).copy(deep=True)

        # first-pass
        season_professors["professor_id"] = match_professors(
            season_professors, professors
        )

        professors_update = season_professors.drop_duplicates(
            subset=["name", "email"], keep="first"
        ).copy(deep=True)

        new_professors = professors_update[professors_update["professor_id"].isna()]

        max_professor_id = max(list(professors["professor_id"]) + [0])
        new_professor_ids = pd.Series(
            list(
                range(
                    max_professor_id + 1,
                    max_professor_id + len(new_professors) + 1,
                )
            ),
            index=new_professors.index,
            dtype=np.float64,
        )
        # Replace with new IDs
        professors_update.loc[new_professors.index, "professor_id"] = new_professor_ids
        professors_update["professor_id"] = professors_update["professor_id"].astype(
            int
        )
        professors_update.drop_duplicates(
            subset=["professor_id"], keep="first", inplace=True
        )
        professors_update = professors_update.set_index("professor_id")

        professors = professors.set_index("professor_id", drop=True)
        professors = professors_update[professors.columns].combine_first(professors)
        professors = professors.reset_index(drop=False)

        # second-pass
        season_professors["professor_id"] = match_professors(
            season_professors, professors
        )

        course_professors_.append(season_professors[["course_id", "professor_id"]])

    course_professors = pd.concat(course_professors_, axis=0, sort=True)

    return professors, course_professors


# for memory profiling
def import_courses(
    merged_course_info: pd.DataFrame, seasons: list[str]
) -> tuple[pd.DataFrame, ...]:
    """
    Import courses into Pandas DataFrames.

    Parameters
    ----------
    merged_course_info:
        Raw course information from JSON files.
    seasons:
        List of seasons for sorting purposes.

    Returns
    -------
    courses, listings, course_professors, professors
    """
    merged_course_info = resolve_cross_listings(merged_course_info)

    logging.debug("Creating courses table")
    # initialize courses table
    courses = merged_course_info.drop_duplicates(subset="temp_course_id").copy(
        deep=True
    )
    # global course IDs
    courses["course_id"] = range(len(courses))
    # convert to JSON string for postgres
    courses["areas"] = courses["areas"].apply(ujson.dumps)
    courses["times_by_day"] = courses["times_by_day"].apply(ujson.dumps)
    courses["skills"] = courses["skills"].apply(ujson.dumps)
    # replace carriage returns for tsv-based migration
    courses["description"] = courses["description"].apply(lambda x: x.replace("\r", ""))
    courses["title"] = courses["title"].apply(lambda x: x.replace("\r", ""))
    courses["short_title"] = courses["short_title"].apply(lambda x: x.replace("\r", ""))
    courses["requirements"] = courses["requirements"].apply(
        lambda x: x.replace("\r", "")
    )

    logging.debug("Creating listings table")
    # map temporary season-specific IDs to global course IDs
    temp_to_course_id = dict(zip(courses["temp_course_id"], courses["course_id"]))

    # initialize listings table
    listings = merged_course_info.copy(deep=True)
    listings["listing_id"] = range(len(listings))
    listings["course_id"] = listings["temp_course_id"].apply(temp_to_course_id.get)
    listings["section"] = listings["section"].apply(lambda x: "0" if x is None else x)
    listings["section"] = listings["section"].fillna("0").astype(str)
    listings["section"] = listings["section"].replace({"": "0"})

    professors_prep = aggregate_professors(courses)

    professors, course_professors = resolve_professors(professors_prep, seasons)

    # explicitly specify missing columns to be filled in later
    courses[
        [
            "location_times",
            "average_rating",
            "average_rating_n",
            "average_workload",
            "average_workload_n",
            "average_rating_same_professors",
            "average_rating_same_professors_n",
            "average_workload_same_professors",
            "average_workload_same_professors_n",
            "same_course_id",
            "same_course_and_profs_id",
            "last_offered_course_id",
            "last_enrollment_course_id",
            "last_enrollment",
            "last_enrollment_season_code",
            "last_enrollment_same_professors",
        ]
    ] = np.nan
    professors[["average_rating", "average_rating_n"]] = np.nan

    # construct courses and flags mapping
    logging.debug("Adding course flags")
    course_flags = courses[["course_id", "flags"]].copy(deep=True)
    course_flags = course_flags[course_flags["flags"].apply(len) > 0]
    course_flags = course_flags.explode(column="flags")

    flags = course_flags.drop_duplicates(subset=["flags"], keep="first").copy(deep=True)
    flags["flag_text"] = flags["flags"]
    flags["flag_id"] = range(len(flags))

    flag_text_to_id = dict(zip(flags["flag_text"], flags["flag_id"]))
    course_flags["flag_id"] = course_flags["flags"].apply(flag_text_to_id.get)

    # extract columns to match database
    courses = courses.loc[:, get_table_columns(database.Course)]
    listings = listings.loc[:, get_table_columns(database.Listing)]
    course_professors = course_professors.loc[
        :, get_table_columns(database.course_professors, not_class=True)
    ]
    professors = professors.loc[:, get_table_columns(database.Professor)]
    flags = flags.loc[:, get_table_columns(database.Flag)]
    course_flags = course_flags.loc[
        :, get_table_columns(database.course_flags, not_class=True)
    ]

    return courses, listings, course_professors, professors, course_flags, flags


def match_evaluations_to_courses(
    evaluation_narratives: pd.DataFrame,
    evaluation_ratings: pd.DataFrame,
    evaluation_statistics: pd.DataFrame,
    listings: pd.DataFrame,
) -> tuple[pd.DataFrame, ...]:
    """
    Match evaluations to course IDs.

    Parameters
    ----------
    evaluation_narratives:
        DataFrame of narratives.
    evaluation_ratings:
        DataFrame of ratings.
    evaluation_statistics:
        DataFrame of statistics.
    listings:
        Listings DataFrame from import_courses.

    Returns
    -------
    evaluation_narratives,
    evaluation_ratings,
    evaluation_statistics,
    evaluation_questions
    """
    logging.debug("Matching evaluations to courses")

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

    def get_course_id(row):
        course_id = season_crn_to_course_id.get(row["season"], {}).get(row["crn"], None)
        return course_id

    # get course IDs
    evaluation_narratives["course_id"] = evaluation_narratives.apply(
        get_course_id, axis=1
    )
    evaluation_ratings["course_id"] = evaluation_ratings.apply(get_course_id, axis=1)
    evaluation_statistics["course_id"] = evaluation_statistics.apply(
        get_course_id, axis=1
    )

    # each course must have exactly one statistic, so use this for reporting
    nan_total = evaluation_statistics["course_id"].isna().sum()
    logging.debug(
        f"Removing {nan_total}/{len(evaluation_statistics)} evaluated courses without matches"
    )

    # remove unmatched courses
    evaluation_narratives.dropna(subset=["course_id"], axis=0, inplace=True)
    evaluation_ratings.dropna(subset=["course_id"], axis=0, inplace=True)
    evaluation_statistics.dropna(subset=["course_id"], axis=0, inplace=True)

    # change from float to integer type for import
    evaluation_narratives["course_id"] = evaluation_narratives["course_id"].astype(int)
    evaluation_ratings["course_id"] = evaluation_ratings["course_id"].astype(int)
    evaluation_statistics["course_id"] = evaluation_statistics["course_id"].astype(int)

    # drop cross-listing duplicates
    evaluation_statistics.drop_duplicates(
        subset=["course_id"], inplace=True, keep="first"
    )
    evaluation_ratings.drop_duplicates(
        subset=["course_id", "question_code"], inplace=True, keep="first"
    )
    evaluation_narratives["comment"] = (
        evaluation_narratives["comment"]
        .str.replace("\r", " ")
        .str.replace("\n", " ")
        .str.replace("  ", " ")
    )
    evaluation_narratives.drop_duplicates(
        subset=["course_id", "question_code", "comment"],
        inplace=True,
        keep="first",
    )

    return evaluation_statistics, evaluation_narratives, evaluation_ratings


def import_evaluations(
    parsed_evaluations_dir: Path,
    listings: pd.DataFrame,
) -> tuple[pd.DataFrame, ...]:
    """
    Import course evaluations into Pandas DataFrame.

    Parameters
    ----------
    parsed_evaluations_dir:
        Directory containing parsed evaluations.
    listings:
        Table of listings from import_courses.

    Returns
    -------
    evaluation_narratives,
    evaluation_ratings,
    evaluation_statistics,
    evaluation_questions
    """
    evaluation_narratives = pd.read_csv(
        parsed_evaluations_dir / "evaluation_narratives.csv",
        dtype={"season": int, "crn": int},
    )
    evaluation_ratings = pd.read_csv(
        parsed_evaluations_dir / "evaluation_ratings.csv",
        dtype={"season": int, "crn": int},
    )
    evaluation_statistics = pd.read_csv(
        parsed_evaluations_dir / "evaluation_statistics.csv",
        dtype={"season": int, "crn": int},
    )
    evaluation_questions = pd.read_csv(
        parsed_evaluations_dir / "evaluation_questions.csv",
        dtype={"season": int, "crn": int},
    )
    # parse rating objects
    evaluation_ratings["rating"] = evaluation_ratings["rating"].apply(ujson.loads)

    (
        evaluation_statistics,
        evaluation_narratives,
        evaluation_ratings,
    ) = match_evaluations_to_courses(
        evaluation_narratives,
        evaluation_ratings,
        evaluation_statistics,
        listings,
    )

    # -------------------
    # Aggregate questions
    # -------------------

    # consistency checks
    logging.debug("Checking question text consistency")
    text_by_code = cast(
        pd.Series,
        evaluation_questions.groupby("question_code")["question_text"].apply(set),
    )

    # focus on question texts with multiple variations
    text_by_code = text_by_code[text_by_code.apply(len) > 1]

    def amend_texts(texts: set[str]) -> set[str]:
        """
        Remove extraneous texts.

        Parameters
        ----------
        texts:
            Set of texts to amend.
        """

        for remove_text in REMOVE_TEXTS:

            texts = {text.replace(remove_text, "") for text in texts}

        return texts

    text_by_code = text_by_code.apply(amend_texts)

    # add [0] at the end to account for empty lists
    max_diff_texts = max(list(text_by_code.apply(len)) + [0])
    logging.debug(
        f"Maximum number of different texts per question code: {max_diff_texts}"
    )

    # get the maximum distance between a set of texts
    def max_pairwise_distance(texts: set[str]):

        pairs = combinations(texts, 2)
        distances = [textdistance.levenshtein.distance(*pair) for pair in pairs]

        return max(distances)

    distances_by_code: pd.Series[float] = text_by_code.apply(max_pairwise_distance)
    # add [0] at the end to account for empty lists
    max_all_distances = max(list(distances_by_code) + [0])

    logging.debug(f"Maximum text divergence within codes: {max_all_distances}")

    if not all(distances_by_code < QUESTION_DIVERGENCE_CUTOFF):
        inconsistent_codes = ", ".join(
            [
                str(x)
                for x in distances_by_code[
                    distances_by_code >= QUESTION_DIVERGENCE_CUTOFF
                ].index
            ]
        )

        raise database.InvariantError(
            f"Error: question codes {inconsistent_codes} have divergent texts"
        )

    logging.debug("Checking question type (narrative/rating) consistency")
    is_narrative_by_code = evaluation_questions.groupby("question_code")[
        "is_narrative"
    ].apply(set)

    # check that a question code is always narrative or always rating
    if not all(is_narrative_by_code.apply(len) == 1):
        inconsistent_codes = ", ".join(
            [
                str(x)
                for x in is_narrative_by_code[
                    is_narrative_by_code.apply(len) != 1
                ].index
            ]
        )
        raise database.InvariantError(
            f"Error: question codes {inconsistent_codes} have both narratives and ratings"
        )

    # deduplicate questions and keep most recent
    evaluation_questions = evaluation_questions.sort_values(
        by="season", ascending=False
    )
    evaluation_questions.drop_duplicates(
        subset=["question_code"], keep="first", inplace=True
    )

    evaluation_questions["options"] = evaluation_questions["options"].replace(
        "NaN", "[]"
    )

    # -------------------
    # Clean up and subset
    # -------------------

    # evaluation narratives ----------------

    # filter out missing or short comments
    evaluation_narratives.dropna(subset=["comment"], inplace=True)

    # MIN_COMMENT_LENGTH = 2
    evaluation_narratives = evaluation_narratives.loc[
        evaluation_narratives["comment"].apply(len) > 2
    ].copy()
    # replace carriage returns for csv-based migration
    evaluation_narratives.loc[:, "comment"] = evaluation_narratives["comment"].apply(
        lambda x: x.replace("\r", "")
    )
    # id column for database primary key
    evaluation_narratives.loc[:, "id"] = list(range(len(evaluation_narratives)))
    evaluation_narratives.reset_index(drop=True, inplace=True)

    # evaluation ratings ----------------

    # id column for database primary key
    evaluation_ratings.loc[:, "id"] = list(range(len(evaluation_ratings)))
    evaluation_ratings.reset_index(drop=True, inplace=True)

    # evaluation questions ----------------

    # tag to be added later
    evaluation_questions["tag"] = ""
    evaluation_questions.reset_index(drop=True, inplace=True)

    # evaluation statistics ----------------

    # explicitly specify missing columns to be filled in later
    evaluation_statistics[["avg_rating", "avg_workload", "enrollment"]] = np.nan
    # convert to JSON string for postgres
    evaluation_statistics.loc[:, "extras"] = evaluation_statistics["extras"].apply(
        ujson.dumps
    )
    evaluation_statistics.reset_index(drop=True, inplace=True)

    # extract columns to match database  ----------------
    evaluation_narratives = evaluation_narratives.loc[
        :, get_table_columns(database.EvaluationNarrative)
    ]
    evaluation_ratings = evaluation_ratings.loc[
        :, get_table_columns(database.EvaluationRating)
    ]
    evaluation_statistics = evaluation_statistics.loc[
        :, get_table_columns(database.EvaluationStatistics)
    ]
    evaluation_questions = evaluation_questions.loc[
        :, get_table_columns(database.EvaluationQuestion)
    ]

    return (
        evaluation_narratives,
        evaluation_ratings,
        evaluation_statistics,
        evaluation_questions,
    )

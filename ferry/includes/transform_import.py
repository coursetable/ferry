"""
Functions for importing information into tables.
Used by /ferry/transform.py.
"""
from collections import Counter
from itertools import combinations
from typing import List, Tuple

import numpy as np
import pandas as pd
import textdistance
import ujson

from ferry import database
from ferry.includes.utils import (
    get_table_columns,
    invert_dict_of_lists,
    merge_overlapping,
)

# maximum question divergence to allow
QUESTION_DIVERGENCE_CUTOFF = 32


def resolve_cross_listings(merged_course_info: pd.DataFrame) -> pd.DataFrame:

    """
    Resolve course cross-listings by computing unique course_ids.

    Parameters
    ----------
    merged_course_info:
        Raw course information from JSON files

    Returns
    -------
    merged_course_info with 'temp_course_id' field added

    """

    # seasons must be sorted in ascending order
    # prioritize Yale College courses when deduplicating listings
    print("Sorting by season and if-undergrad")

    def classify_yc(row):
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

    print("Aggregating cross-listings")
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

    print("Mapping out cross-listings")
    # map CRN groups to temporary IDs within each season
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

    return merged_course_info


def aggregate_professors(courses: pd.DataFrame) -> pd.DataFrame:

    """
    Aggregate professor info columns in preparation for matching.

    Parameters
    ----------
    courses:
        intermediate courses table from import_courses()

    Returns
    -------
    professors_prep: professor attributes DataFrame

    """

    print("Creating professors table")
    # initialize professors table
    professors_prep = courses.loc[
        :,
        ["season_code", "course_id", "professors", "professor_emails", "professor_ids"],
    ]

    print("Resolving professor attributes")
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
    professors_prep["professors_info"] = professors_prep[
        ["professors", "professor_emails", "professor_ids"]
    ].to_dict(orient="split")["data"]

    def zip_professors_info(professors_info):
        # helper function to convert professors_info
        # from [[names...],[emails...],[ocs_ids...]] format
        # to [[name,email,ocs_id]...] format

        names, emails, ocs_ids = professors_info

        # exclude empty attributes
        names = list(filter(lambda x: x != "", names))
        emails = list(filter(lambda x: x != "", emails))
        ocs_ids = list(filter(lambda x: x != "", ocs_ids))

        # if no names, return empty regardless of others
        # (professors need to be named)
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

    # exclude instances with empty/bad professor infos
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

    return professors_prep


def resolve_professors(
    professors_prep: pd.DataFrame, seasons: List[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    """
    Resolve course-professor mappings and professors table

    Parameters
    ----------
    professors_prep:
        professor attributes from aggregate_professors()
    seasons:
        list of seasons for sorting purposes

    Returns
    -------
    professors, course_professors

    """

    print("Constructing professors table in chronological order")

    professors = pd.DataFrame(columns=["professor_id", "name", "email", "ocs_id"])

    professors_by_season = professors_prep.groupby("season_code")

    def get_professor_identifiers(professors):
        # return dictionaries mapping professors to
        # professor_id primary keys by names, emails, and ocs_ids

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
        # match professors within a season to main professors

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

        # aggregate found IDs
        season_professors["matched_ids_aggregate"] = (
            season_professors["name_matched_ids"]
            + season_professors["email_matched_ids"]
            + season_professors["ocs_matched_ids"]
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

        tied_professors = season_professors[ties]

        def print_ties(row):
            print(
                f"Professor {row['name']} ({row['email']}, {row['ocs_id']}) has tied matches: ",
                end="",
            )
            print(sorted(list(set(row["matched_ids_aggregate"]))))

        tied_professors.apply(print_ties, axis=1)

        return professor_ids

    # course-professors junction table
    # store as list of DataFrames before concatenation
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

        course_professors.append(season_professors[["course_id", "professor_id"]])

    course_professors = pd.concat(course_professors, axis=0, sort=True)

    return professors, course_professors


# for memory profiling
def import_courses(
    merged_course_info: pd.DataFrame, seasons: List[str]
) -> Tuple[pd.DataFrame, ...]:
    """
    Import courses into Pandas DataFrames.

    Parameters
    ----------
    merged_course_info:
        Raw course information from JSON files
    seasons:
        list of seasons for sorting purposes

    Returns
    -------
    courses, listings, course_professors, professors

    """

    merged_course_info = resolve_cross_listings(merged_course_info)

    print("Creating courses table")
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

    print("Creating listings table")
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
            "average_workload",
            "last_offered_course_id",
            "last_enrollment_course_id",
            "last_enrollment",
            "last_enrollment_season_code",
            "last_enrollment_same_professors",
        ]
    ] = np.nan
    professors["average_rating"] = np.nan

    # construct courses and flags mapping
    print("Adding course flags")
    course_flags = courses[["course_id", "flags"]].copy(deep=True)
    course_flags = course_flags[course_flags["flags"].apply(len) > 0]
    course_flags = course_flags.explode("flags")

    flags = course_flags.drop_duplicates(subset=["flags"], keep="first").copy(deep=True)
    flags["flag_text"] = flags["flags"]
    flags["flag_id"] = range(len(flags))

    flag_text_to_id = dict(zip(flags["flag_text"], flags["flag_id"]))
    course_flags["flag_id"] = course_flags["flags"].apply(flag_text_to_id.get)

    # extract columns to match database
    courses = courses.loc[:, get_table_columns(database.models.Course)]
    listings = listings.loc[:, get_table_columns(database.models.Listing)]
    course_professors = course_professors.loc[
        :, get_table_columns(database.models.course_professors, not_class=True)
    ]
    professors = professors.loc[:, get_table_columns(database.models.Professor)]
    flags = flags.loc[:, get_table_columns(database.models.Flag)]
    course_flags = course_flags.loc[
        :, get_table_columns(database.models.course_flags, not_class=True)
    ]

    print("[Summary]")
    print(f"Total courses: {len(courses)}")
    print(f"Total listings: {len(listings)}")
    print(f"Total course-professors: {len(course_professors)}")
    print(f"Total professors: {len(professors)}")
    print(f"Total course-flags: {len(course_flags)}")
    print(f"Total flags: {len(flags)}")

    return courses, listings, course_professors, professors, course_flags, flags


def import_demand(
    merged_demand_info: pd.DataFrame, listings: pd.DataFrame
) -> pd.DataFrame:
    """
    Import demand statistics into Pandas DataFrame.

    Parameters
    ----------
    merged_demand_info:
        Raw demand information from JSON files
    listings:
        Listings table from import_courses

    Returns
    -------
    demand_statistics

    """

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
    demand_statistics["latest_demand"], demand_statistics["latest_demand_date"] = zip(
        *demand_statistics.apply(get_most_recent_demand, axis=1)
    )

    # expand course_id list to one per row
    demand_statistics = demand_statistics.explode("course_id")
    demand_statistics.drop_duplicates(subset=["course_id"], keep="first", inplace=True)

    # rename demand JSON column to match database
    demand_statistics = demand_statistics.rename(
        {"overall_demand": "demand"}, axis="columns"
    )

    demand_statistics["demand"] = demand_statistics["demand"].apply(ujson.dumps)

    # extract columns to match database
    demand_statistics = demand_statistics.loc[
        :, get_table_columns(database.models.DemandStatistics)
    ]

    print("[Summary]")
    print(f"Total demand statistics: {len(demand_statistics)}")

    return demand_statistics


def match_evaluations_to_courses(
    evaluation_narratives: pd.DataFrame,
    evaluation_ratings: pd.DataFrame,
    evaluation_statistics: pd.DataFrame,
    listings: pd.DataFrame,
) -> Tuple[pd.DataFrame, ...]:

    """
    Match evaluations to course IDs.

    Parameters
    ----------
    evaluation_narratives:
        DataFrame of narratives
    evaluation_ratings:
        DataFrame of ratings
    evaluation_statistics:
        DataFrame of statistics
    listings:
        listings DataFrame from import_courses

    Returns
    -------
    evaluation_narratives,
    evaluation_ratings,
    evaluation_statistics,
    evaluation_questions

    """

    print("Matching evaluations to courses")

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
    print(
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
    evaluation_narratives.drop_duplicates(
        subset=["course_id", "question_code", "comment"], inplace=True, keep="first"
    )

    return evaluation_statistics, evaluation_narratives, evaluation_ratings


def import_evaluations(
    evaluation_narratives: pd.DataFrame,
    evaluation_ratings: pd.DataFrame,
    evaluation_statistics: pd.DataFrame,
    evaluation_questions: pd.DataFrame,
    listings: pd.DataFrame,
) -> Tuple[pd.DataFrame, ...]:
    """
    Import course evaluations into Pandas DataFrame.

    Parameters
    ----------
    evaluation_narratives:
        Table of narratives from /ferry/crawler/parse_ratings.py
    evaluation_ratings:
        Table of ratings from /ferry/crawler/parse_ratings.py
    evaluation_statistics:
        Table of statistics from /ferry/crawler/parse_ratings.py
    evaluation_questions:
        Table of questions from /ferry/crawler/parse_ratings.py
    listings:
        Table of listings from import_courses

    Returns
    -------
    evaluation_narratives,
    evaluation_ratings,
    evaluation_statistics,
    evaluation_questions

    """

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
    print("Checking question text consistency")
    text_by_code = evaluation_questions.groupby("question_code")["question_text"].apply(
        set
    )

    # focus on question texts with multiple variations
    text_by_code = text_by_code[text_by_code.apply(len) > 1]

    # add [0] at the end to account for empty lists
    max_diff_texts = max(list(text_by_code.apply(len)) + [0])
    print(f"Maximum number of different texts per question code: {max_diff_texts}")

    # get the maximum distance between a set of texts
    def max_pairwise_distance(texts):

        pairs = combinations(texts, 2)
        distances = [textdistance.levenshtein.distance(*pair) for pair in pairs]

        return max(distances)

    distances_by_code = text_by_code.apply(max_pairwise_distance)
    # add [0] at the end to account for empty lists
    max_all_distances = max(list(distances_by_code) + [0])

    print(f"Maximum text divergence within codes: {max_all_distances}")

    if not all(distances_by_code < QUESTION_DIVERGENCE_CUTOFF):

        inconsistent_codes = distances_by_code[
            distances_by_code >= QUESTION_DIVERGENCE_CUTOFF
        ]
        inconsistent_codes = list(inconsistent_codes.index)
        inconsistent_codes = ", ".join(inconsistent_codes)

        raise database.InvariantError(
            f"Error: question codes {inconsistent_codes} have divergent texts"
        )

    print("Checking question type (narrative/rating) consistency")
    is_narrative_by_code = evaluation_questions.groupby("question_code")[
        "is_narrative"
    ].apply(set)

    # check that a question code is always narrative or always rating
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
    ]
    # replace carriage returns for csv-based migration
    evaluation_narratives.loc[:, "comment"] = evaluation_narratives["comment"].apply(
        lambda x: x.replace("\r", "")
    )
    # id column for database primary key
    evaluation_narratives.loc[:, "id"] = range(len(evaluation_narratives))
    evaluation_narratives.reset_index(drop=True, inplace=True)

    # evaluation ratings ----------------

    # id column for database primary key
    evaluation_ratings.loc[:, "id"] = range(len(evaluation_ratings))
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
        :, get_table_columns(database.models.EvaluationNarrative)
    ]
    evaluation_ratings = evaluation_ratings.loc[
        :, get_table_columns(database.models.EvaluationRating)
    ]
    evaluation_statistics = evaluation_statistics.loc[
        :, get_table_columns(database.models.EvaluationStatistics)
    ]
    evaluation_questions = evaluation_questions.loc[
        :, get_table_columns(database.models.EvaluationQuestion)
    ]

    print("[Summary]")
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

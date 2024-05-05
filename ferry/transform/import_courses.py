import logging
from tqdm import tqdm
from pathlib import Path
from collections import Counter

import numpy as np
import pandas as pd
import ujson

from ferry import database
from ferry.utils import (
    get_table_columns,
    to_element_index_map,
    merge_overlapping,
)


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


def resolve_cross_listings(merged_course_info: pd.DataFrame) -> pd.DataFrame:
    """
    Resolve course cross-listings by computing unique course_ids.

    It creates a new column, `temp_course_id`, a globally unique string ID for
    each course, formed with season + a season-unique numeric ID for each course.
    """

    # seasons must be sorted in ascending order
    # prioritize Yale College courses when deduplicating listings
    logging.debug("Sorting by season and if-undergrad")

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
    # crns_by_season[season_code] -> Series[Series[CRN]]
    crns_by_season = merged_course_info.groupby("season_code")["crns"]
    # crns_by_season[season_code] -> list[frozenset[CRN]]
    crns_by_season = crns_by_season.apply(lambda x: [frozenset(y) for y in x])
    # crns_by_season[season_code] -> list[set[CRN]]
    crns_by_season = crns_by_season.apply(merge_overlapping)

    logging.debug("Mapping out cross-listings")
    # temp_course_ids_by_season[season_code][CRN] -> course_id
    temp_course_ids_by_season = crns_by_season.apply(to_element_index_map).to_dict()

    # temporary string-based unique course identifier
    merged_course_info["temp_course_id"] = merged_course_info.apply(
        lambda row: f"{row['season_code']}_{temp_course_ids_by_season[row['season_code']][row['crn']]}",
        axis=1,
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
    professors_prep: professor attributes DataFrame, with 4 columns:
        - season_code
        - course_id
        - name
        - email
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
    def aggregate_prof_info(row: pd.Series) -> list[tuple[str, str | None]]:
        names, emails = row["professors"], row["professor_emails"]

        names: list[str] = list(filter(lambda x: x != "", names))
        emails: list[str | None] = list(filter(lambda x: x != "", emails))

        # if no names, return empty regardless of others
        # (professors need to be named)
        if len(names) == 0:
            return []

        # account for inconsistent lengths before zipping
        if len(emails) != len(names):
            emails = [None] * len(names)

        return list(zip(names, emails))

    professors_prep["professors_info"] = professors_prep.apply(
        aggregate_prof_info, axis=1
    )

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
    all_season_professors = []

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
            np.arange(
                max_professor_id + 1,
                max_professor_id + len(new_professors) + 1,
            ),
            index=new_professors.index,
            dtype=int,
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

        all_season_professors.append(season_professors[["course_id", "professor_id"]])

    course_professors = pd.concat(all_season_professors, axis=0, sort=True)

    return professors, course_professors


def import_courses(
    parsed_courses_dir: Path, migrated_courses_dir: Path, seasons: list[str]
) -> dict[str, pd.DataFrame]:
    """
    Import courses from JSON files in `parsed_courses_dir` and `migrated_courses_dir`.
    Splits the raw data into various tables for the database.

    Returns
    -------
    - courses: corresponds to database.Course; deduplicated by cross-listings
    - listings: corresponds to database.Listing
    - course_professors: corresponds to database.course_professors
    - professors: corresponds to database.Professor
    - course_flags: corresponds to database.course_flags
    - flags: corresponds to database.Flag
    """

    print("\nImporting courses...")
    all_course_info: list[pd.DataFrame] = []

    for season in tqdm(seasons, desc="Loading course JSONs", leave=False):
        # Read the course listings, giving preference to freshly parsed over migrated ones.
        parsed_courses_file = parsed_courses_dir / f"{season}.json"
        if not parsed_courses_file.is_file():
            # check migrated courses as a fallback
            parsed_courses_file = migrated_courses_dir / f"{season}.json"
        if not parsed_courses_file.is_file():
            print(f"Skipping season {season}: not found in parsed or migrated courses.")
            continue
        parsed_course_info = pd.read_json(parsed_courses_file)
        parsed_course_info["season_code"] = season
        all_course_info.append(parsed_course_info)

    merged_course_info = pd.concat(all_course_info, axis=0).reset_index(drop=True)
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
    listings["section"] = listings["section"].fillna("0").astype(str).replace({"": "0"})

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

    print("\033[F", end="")
    print("Importing courses... ✔")

    print("[Summary]")
    print(f"Total courses: {len(courses)}")
    print(f"Total listings: {len(listings)}")
    print(f"Total course-professors: {len(course_professors)}")
    print(f"Total professors: {len(professors)}")
    print(f"Total course-flags: {len(course_flags)}")
    print(f"Total flags: {len(flags)}")

    return {
        "courses": courses,
        "listings": listings,
        "course_professors": course_professors,
        "professors": professors,
        "course_flags": course_flags,
        "flags": flags,
    }

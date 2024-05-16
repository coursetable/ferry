import logging
from tqdm import tqdm
from typing import TypedDict, Any, Iterable
from pathlib import Path

import pandas as pd
import ujson


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
    temp_course_ids_by_season = {}
    for season, crns_of_season in merged_course_info.groupby("season_code")["crns"]:
        temp_course_id = 0
        crn_to_course_id = {}
        for crns in crns_of_season:
            existing_ids = set(
                crn_to_course_id[crn] if crn in crn_to_course_id else None
                for crn in crns
            )
            if existing_ids == {None}:
                for crn in crns:
                    crn_to_course_id[crn] = f"{season}_{temp_course_id}"
                temp_course_id += 1
            elif len(existing_ids) > 1:
                raise SystemExit(
                    f"Unexpected: {crns} are partially matched in {season}. The CRN graph should be a disjoint union of cliques."
                )
        temp_course_ids_by_season[season] = crn_to_course_id

    # temporary string-based unique course identifier
    merged_course_info["temp_course_id"] = merged_course_info.apply(
        lambda row: temp_course_ids_by_season[row["season_code"]][row["crn"]],
        axis=1,
    )

    return merged_course_info


def aggregate_professors(courses: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Two professors are deemed the same if:

    - The emails are equal and both non-empty. In this case we pick the name that
      appears last in the table. OR
    - At least one of the two entries' email is empty and the names are equal. In
      this case we pick the non-empty email if there's one.

    This means:

    - If a professor has multiple emails, they will be treated as separate professors.
      (Usually this means they are two people with the same name.)
    - If a professor changes their registered name, and all emails are empty, they will
      be treated as separate professors.
    - For two professors with the same name, entries with empty emails will be randomly
      attributed to either professor.

    Theoretically, OCS provides an ID (in the `professor_ids` field that we scraped).
    At one point we also used the ID to do matching. However, it turns out that Yale
    recycles OCS IDs, so we can't use it without a bunch of wrong matches. Legacy
    courses also have no professor ID anyway.
    """
    logging.debug("Aggregating professor attributes")

    course_professors = (
        courses[["course_id", "professors", "professor_emails"]]
        .explode(["professors", "professor_emails"])
        .dropna(subset="professors")
        .rename(columns={"professors": "name", "professor_emails": "email"})
        .reset_index(drop=True)
    )
    # First: try to fill empty emails
    course_professors = course_professors.groupby("name")

    def fix_empty_email(group: pd.DataFrame) -> pd.DataFrame:
        first_valid_email = next((s for s in group["email"] if s), None)
        if first_valid_email is None:
            return group
        group["email"] = group["email"].replace({"": first_valid_email})
        all_emails = group["email"].unique()
        if len(all_emails) > 1:
            logging.warning(
                f"Multiple emails with name {group.name}: {all_emails}; they will be treated as separate professors"
            )
        return group

    # Second: deduplicate by email, falling back to name
    course_professors = course_professors.apply(fix_empty_email).reset_index(drop=True)

    def warn_different_name(group: pd.DataFrame):
        all_names = group["name"].unique()
        if group.name != "" and len(all_names) > 1:
            logging.warning(
                f"Multiple names with email {group.name}: {all_names}; only the last name will be used"
            )

    course_professors.groupby("email").apply(warn_different_name)

    course_professors["professor_id"] = course_professors.apply(
        lambda x: x["email"] or x["name"], axis=1
    )
    course_professors["professor_id"] = course_professors.groupby(
        "professor_id"
    ).ngroup()
    professors = course_professors.drop_duplicates(
        subset="professor_id", keep="last"
    ).copy(deep=True)
    professors["email"] = professors["email"].replace({"": None})
    return professors, course_professors


def aggregate_flags(courses: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logging.debug("Adding course flags")
    course_flags = (
        courses[["course_id", "flags"]]
        .explode(column="flags")
        .dropna(subset="flags")
        .rename(columns={"flags": "flag_text"})
        .reset_index(drop=True)
    )

    course_flags["flag_id"] = course_flags.groupby("flag_text").ngroup()
    flags = course_flags.drop_duplicates(subset="flag_id")
    return flags, course_flags


class CourseTables(TypedDict):
    courses: pd.DataFrame
    listings: pd.DataFrame
    course_professors: pd.DataFrame
    professors: pd.DataFrame
    course_flags: pd.DataFrame
    flags: pd.DataFrame


def import_courses(parsed_courses_dir: Path, seasons: list[str]) -> CourseTables:
    """
    Import courses from JSON files in `parsed_courses_dir`.
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
    all_imported_listings: list[pd.DataFrame] = []

    for season in tqdm(seasons, desc="Loading course JSONs", leave=False):
        parsed_courses_file = parsed_courses_dir / f"{season}.json"
        if not parsed_courses_file.is_file():
            print(f"Skipping season {season}: not found in parsed courses.")
            continue
        parsed_course_info = pd.read_json(parsed_courses_file, dtype={"crn": int})
        parsed_course_info["season_code"] = season
        all_imported_listings.append(parsed_course_info)

    imported_listings = pd.concat(all_imported_listings, axis=0).reset_index(drop=True)
    imported_listings["crns"] = imported_listings["crns"].apply(
        lambda crns: [int(crn) for crn in crns]
    )
    # convert to JSON string for postgres
    imported_listings["times_by_day"] = imported_listings["times_by_day"].apply(
        ujson.dumps
    )
    imported_listings["skills"] = imported_listings["skills"].apply(ujson.dumps)
    imported_listings["areas"] = imported_listings["areas"].apply(ujson.dumps)
    imported_listings["section"] = (
        imported_listings["section"].fillna("0").astype(str).replace({"": "0"})
    )
    imported_listings = resolve_cross_listings(imported_listings)

    logging.debug("Creating courses table")
    # initialize courses table
    courses = imported_listings.drop_duplicates(subset="temp_course_id").copy(deep=True)
    # global course IDs
    courses["course_id"] = range(len(courses))

    logging.debug("Creating listings table")
    listings = pd.merge(
        imported_listings,
        courses[["temp_course_id", "course_id"]],
        on="temp_course_id",
        how="left",
    )
    listings["listing_id"] = range(len(listings))

    professors, course_professors = aggregate_professors(courses)
    flags, course_flags = aggregate_flags(courses)

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

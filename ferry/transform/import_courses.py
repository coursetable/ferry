import logging
from tqdm import tqdm
from typing import TypedDict, cast, Callable
from pathlib import Path
from ..crawler.classes.parse import ParsedMeeting

import numpy as np
import pandas as pd
import ujson
from ferry.crawler.cache import load_cache_json


def generate_id(
    df: pd.DataFrame, get_cache_key: Callable[[pd.Series], str], cache_path: Path
) -> pd.Series:
    """
    Generate a unique ID for each row in a DataFrame.

    `column_name` identifies each unique row value. `id_cache` stores existing
    mappings of `column_name` to ID. Then, for all unmapped rows, they are
    assigned IDs in increasing values.
    """
    id_cache: dict[str, int] = load_cache_json(cache_path) or {}
    cache_keys = df.apply(get_cache_key, axis=1)
    ids = cache_keys.map(id_cache)
    max_flag_id = max(id_cache.values(), default=0)
    unmapped = cache_keys[ids.isna()].unique()
    new_flag_ids = pd.Series(range(max_flag_id + 1, max_flag_id + 1 + len(unmapped)))
    return ids.fillna(cache_keys.map(dict(zip(unmapped, new_flag_ids)))).astype(int)


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


def resolve_cross_listings(listings: pd.DataFrame, data_dir: Path) -> pd.DataFrame:
    """
    Resolve course cross-listings by computing unique course_ids.

    It creates a new column, `temp_course_id`, a globally unique string ID for
    each course, formed with season + a season-unique numeric ID for each course.
    """

    # seasons must be sorted in ascending order
    # prioritize Yale College courses when deduplicating listings
    logging.debug("Sorting by season and if-undergrad")

    course_id_cache: dict[str, int] = (
        load_cache_json(data_dir / "id_cache" / "course_id.json") or {}
    )

    listings["is_yc"] = listings.apply(classify_yc, axis=1)
    listings = listings.sort_values(
        by=["season_code", "is_yc"], ascending=[True, False]
    )

    logging.debug("Aggregating cross-listings")
    temp_course_ids_by_season: dict[str, dict[int, str]] = {}
    for season, crns_of_season in listings.groupby("season_code")["crns"]:
        temp_course_id = 0
        crn_to_course_id: dict[int, str] = {}
        for crns in crns_of_season:
            existing_ids = set(map(crn_to_course_id.get, crns))
            if existing_ids == {None}:
                for crn in crns:
                    crn_to_course_id[crn] = f"{season}_{temp_course_id}"
                temp_course_id += 1
            elif len(existing_ids) > 1:
                raise ValueError(
                    f"Unexpected: {crns} are matched to multiple courses in {season}. The CRN graph should be a disjoint union of cliques."
                )
        temp_course_ids_by_season[cast(str, season)] = crn_to_course_id

    # temporary string-based unique course identifier
    listings["temp_course_id"] = listings.apply(
        lambda row: temp_course_ids_by_season[row["season_code"]][row["crn"]],
        axis=1,
    )

    next_course_id = max(course_id_cache.values(), default=0)
    course_ids_assigned: set[int] = set()

    def listing_group_to_id(group: pd.DataFrame) -> int:
        nonlocal next_course_id
        all_seasons = group["season_code"].unique()
        if len(all_seasons) > 1:
            raise ValueError(
                f"Unexpected: {group['temp_course_id']} is matched to multiple seasons: {all_seasons}"
            )
        season = all_seasons[0]
        all_course_ids = set(
            course_id_cache.get(f"{season}-{crn}") for crn in group["crn"]
        )
        all_course_ids.discard(None)
        if len(all_course_ids) > 1:
            logging.warning(
                f"The following courses are mapped to multiple courses: {all_course_ids}:\n{listings.loc[group['temp_course_id'].index][['season_code', 'title', 'course_code', 'crns']]}\nThey will be merged into the first one"
            )
        already_assigned_ids = all_course_ids & course_ids_assigned
        if already_assigned_ids:
            logging.warning(
                f"Course ID {already_assigned_ids} is already used by another group; probably because cross-listings are split"
            )
        unassigned_ids = all_course_ids - course_ids_assigned
        if unassigned_ids:
            id = cast(int, unassigned_ids.pop())
            course_ids_assigned.add(id)
            return id
        next_course_id += 1
        course_ids_assigned.add(next_course_id)
        return next_course_id

    course_id = (
        listings.groupby("temp_course_id")
        .apply(listing_group_to_id)
        .reset_index(name="course_id")
    )
    listings = listings.merge(course_id, on="temp_course_id", how="left")
    return listings


def aggregate_professors(
    courses: pd.DataFrame, data_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
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
        courses[["professors", "professor_emails"]]
        .explode(["professors", "professor_emails"])
        .dropna(subset="professors")
        .rename(columns={"professors": "name", "professor_emails": "email"})
        .reset_index(drop=False)
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

    course_professors["professor_id"] = generate_id(
        course_professors,
        lambda x: f"{x['name']} <{x['email']}>" if x["email"] else x["name"],
        data_dir / "id_cache" / "professor_id.json",
    )
    professors = course_professors.drop_duplicates(
        subset="professor_id", keep="last"
    ).set_index("professor_id")
    professors["email"] = professors["email"].replace({"": None})
    return professors, course_professors


def aggregate_flags(
    courses: pd.DataFrame, data_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    logging.debug("Adding course flags")
    course_flags = (
        courses["flags"].explode().dropna().rename("flag_text").reset_index(drop=False)
    )

    course_flags["flag_id"] = generate_id(
        course_flags,
        lambda row: row["flag_text"],
        data_dir / "id_cache" / "flag_id.json",
    )
    flags = course_flags.drop_duplicates(subset="flag_id").set_index("flag_id")
    return flags, course_flags


def parse_location(location: str) -> dict[str, str | None]:
    def do_parse():
        if " - " not in location:
            if " " not in location:
                # Just building code
                return {"building_name": None, "code": location, "room": None}
            # [code] [room]
            code, room = location.split(" ", 1)
            return {"building_name": None, "code": code, "room": room}
        abbrev, rest = location.split(" - ", 1)
        if " " not in abbrev:
            if rest == abbrev:
                rest = None
            # [code] - [building name]
            return {"building_name": rest, "code": abbrev, "room": None}
        code, room = abbrev.split(" ", 1)
        if not rest.endswith(room):
            raise ValueError(f"Unexpected location format: {location}")
        building_full_name = rest.removesuffix(f" {room}")
        if building_full_name == code:
            building_full_name = None
        # [code] [room] - [building name] [room]
        return {"building_name": building_full_name, "code": code, "room": room}

    res = do_parse()
    for key in res:
        if res[key] == "" or res[key] == "TBA":
            res[key] = None
    if res["code"] is None:
        raise ValueError(f"Unexpected location format: {location}")
    return res


def aggregate_locations(
    courses: pd.DataFrame, data_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    location_data = []
    for meetings in courses["meetings"]:
        for meeting in meetings:
            if meeting["location"] in ["", "TBA", "TBA TBA"]:
                continue
            location_data.append({**parse_location(meeting["location"]), "url": meeting["location_url"]})
    locations = pd.DataFrame(location_data).drop_duplicates().reset_index(drop=True)

    def report_multiple_names(row: pd.DataFrame):
        if len(row["building_name"].unique()) > 1:
            logging.warning(
                f"Multiple names for {row.name}: {row['building_name'].unique()}"
            )

    locations[~locations["building_name"].isna()].groupby(["code", "room"]).apply(
        report_multiple_names
    )
    locations = locations.groupby(["code", "room"], as_index=False, dropna=False).last()

    locations["location_id"] = generate_id(
        locations,
        lambda row: f"{row['code']} {'' if pd.isna(row['room']) else row['room']}",
        data_dir / "id_cache" / "location_id.json",
    )
    location_to_id = locations.set_index(["code", "room"])["location_id"].to_dict()
    locations = locations.set_index("location_id")

    buildings = locations.copy(deep=True)
    locations.rename(columns={"code": "building_code"}, inplace=True)

    def report_different_info(group: pd.DataFrame):
        all_names = group["building_name"].unique()
        all_names = all_names[~pd.isna(all_names)]
        if len(all_names) > 1:
            logging.warning(
                f"Multiple building names for building {group.name}: {all_names}; only the last name will be used"
            )
        all_urls = group["url"].unique()
        all_urls = all_urls[~pd.isna(all_urls)]
        if len(all_urls) > 1:
            logging.warning(
                f"Multiple URLs for building {group.name}: {all_urls}; only the last URL will be used"
            )

    buildings.groupby("code").apply(report_different_info)
    buildings = (
        buildings.sort_values(by=["building_name", "url"])
        .groupby("code")
        .last()
        .reset_index()
    )

    # For each meeting, coalesce location and location_url into a location_id
    # (which is None if location is TBA)
    def transform_course_meetings(original_meetings: list[ParsedMeeting]):
        if len(original_meetings) == 0:
            return []
        meetings = []
        for m in original_meetings:
            if m["location"] in ["", "TBA", "TBA TBA"]:
                meetings.append(
                    {
                        "days_of_week": m["days_of_week"],
                        "start_time": m["start_time"],
                        "end_time": m["end_time"],
                        "location_id": None,
                    }
                )
                continue
            location_info = parse_location(m["location"])
            meetings.append(
                {
                    "days_of_week": m["days_of_week"],
                    "start_time": m["start_time"],
                    "end_time": m["end_time"],
                    "location_id": location_to_id[
                        location_info["code"], location_info["room"] or np.nan
                    ],
                }
            )
        return meetings

    course_meetings = courses["meetings"].apply(transform_course_meetings)
    # course_meetings is a series of course_id -> list of dicts.
    # Explode it to get a DataFrame with course_id, days_of_week, start_time, end_time, location_id
    course_meetings = course_meetings.explode().dropna().apply(pd.Series).reset_index()
    # Merge rows with the same start/end/location
    course_meetings = (
        course_meetings.groupby(
            ["course_id", "start_time", "end_time", "location_id"], dropna=False
        )
        .agg({"days_of_week": reduce_days_of_week})
        .reset_index()
    )
    course_meetings["days_of_week"] = course_meetings["days_of_week"].astype(int)
    course_meetings["location_id"] = course_meetings["location_id"].astype(
        pd.Int64Dtype()
    )
    return course_meetings, locations, buildings


def reduce_days_of_week(days_of_week: list[int]) -> int:
    res = 0
    for i in days_of_week:
        res |= i
    return res


class CourseTables(TypedDict):
    courses: pd.DataFrame
    listings: pd.DataFrame
    course_professors: pd.DataFrame
    professors: pd.DataFrame
    course_flags: pd.DataFrame
    flags: pd.DataFrame
    course_meetings: pd.DataFrame
    locations: pd.DataFrame
    buildings: pd.DataFrame


def import_courses(data_dir: Path, seasons: list[str]) -> CourseTables:
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
    parsed_courses_dir = data_dir / "parsed_courses"

    all_imported_listings: list[pd.DataFrame] = []

    for season in tqdm(seasons, desc="Loading course JSONs", leave=False):
        parsed_courses_file = parsed_courses_dir / f"{season}.json"
        if not parsed_courses_file.is_file():
            print(f"Skipping season {season}: not found in parsed courses.")
            continue
        parsed_course_info = pd.read_json(parsed_courses_file, dtype={"crn": int})
        parsed_course_info["season_code"] = season
        all_imported_listings.append(parsed_course_info)

    logging.debug("Creating listings table")
    listings = pd.concat(all_imported_listings, axis=0).reset_index(drop=True)
    listings["crns"] = listings["crns"].apply(lambda crns: [int(crn) for crn in crns])
    # convert to JSON string for postgres
    listings["skills"] = listings["skills"].apply(ujson.dumps)
    listings["areas"] = listings["areas"].apply(ujson.dumps)
    listings["section"] = listings["section"].fillna("0").astype(str).replace({"": "0"})
    listings["listing_id"] = generate_id(
        listings,
        lambda row: f"{row['season_code']}-{row['crn']}",
        data_dir / "id_cache" / "listing_id.json",
    )
    listings = resolve_cross_listings(listings, data_dir)
    # Do this afterwards, because resolve_cross_listings will drop the index
    listings = listings.set_index("listing_id")

    logging.debug("Creating courses table")
    courses = (
        listings.reset_index(drop=True)
        .drop_duplicates(subset="course_id")
        .set_index("course_id")
    )

    professors, course_professors = aggregate_professors(courses, data_dir)
    flags, course_flags = aggregate_flags(courses, data_dir)
    course_meetings, locations, buildings = aggregate_locations(courses, data_dir)

    print("\033[F", end="")
    print("Importing courses... ✔")

    print("[Summary]")
    print(f"Total courses: {len(courses)}")
    print(f"Total listings: {len(listings)}")
    print(f"Total course-professors: {len(course_professors)}")
    print(f"Total professors: {len(professors)}")
    print(f"Total course-flags: {len(course_flags)}")
    print(f"Total flags: {len(flags)}")
    print(f"Total course-meetings: {len(course_meetings)}")
    print(f"Total locations: {len(locations)}")
    print(f"Total buildings: {len(buildings)}")

    return {
        "courses": courses,
        "listings": listings,
        "course_professors": course_professors,
        "professors": professors,
        "course_flags": course_flags,
        "flags": flags,
        "locations": locations,
        "buildings": buildings,
        "course_meetings": course_meetings,
    }

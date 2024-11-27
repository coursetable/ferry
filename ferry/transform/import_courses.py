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


# These classes in their infinite wisdom published two CRNs that are exactly
# identical and messes up with our assumptions
exactly_identical_crns = {
    "201803": (12568, 13263),  # CPLT 942
    "201901": (21500, 22135),  # PLSC 530
    "202403": (15444, 15753),  # MUS 644
}


def resolve_cross_listings(
    listings: pd.DataFrame, data_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Resolve course cross-listings using the `crns` from the parsed courses.
    Creates the `courses` table that identifies connected components of cross-listings.
    """

    course_id_cache: dict[str, int] = (
        load_cache_json(data_dir / "id_cache" / "course_id.json") or {}
    )
    course_id_to_listings: dict[int, list[str]] = {}
    for key, value in course_id_cache.items():
        course_id_to_listings.setdefault(value, []).append(key)

    # seasons must be sorted in ascending order
    # prioritize Yale College courses when deduplicating listings. We assume that
    # YC listings carry the most info (skills/areas, etc.)
    listings["is_yc"] = listings.apply(classify_yc, axis=1)
    listings = listings.sort_values(
        by=["season_code", "is_yc"], ascending=[True, False]
    )
    listings["crns"] = listings["crns"].apply(
        lambda crns: frozenset(int(crn) for crn in crns)
    )

    # Remove exactly identical CRNs by removing their rows as well their
    # references in other rows' `crns` column
    for season, crns in exactly_identical_crns.items():
        first, *rest = crns
        listings = listings[
            ~(listings["crn"].isin(rest) & listings["season_code"].eq(season))
        ]
        cross_listed_crns = listings[
            listings["season_code"].eq(season) & listings["crn"].eq(first)
        ]["crns"].iloc[0]
        for other_crn in cross_listed_crns - set(crns):
            # Remove rest from the crns of each cross listed crn
            target = listings["season_code"].eq(season) & listings["crn"].eq(other_crn)
            listings.loc[target, "crns"] = listings.loc[target, "crns"].apply(
                lambda x: x - set(rest)
            )

    logging.debug("Aggregating cross-listings")
    # season -> CRN -> set of CRNs it's connected to
    season_crn_graphs: dict[str, dict[int, frozenset[int]]] = (
        listings.groupby("season_code")
        .apply(lambda group: group.set_index("crn")["crns"].to_dict())
        .to_dict()
    )
    next_course_id = max(course_id_cache.values(), default=-1) + 1
    # season_code -> CRN -> course_id
    new_course_ids: dict[str, dict[int, int]] = {}

    # Assign course_id, inheriting from existing course_id if possible
    for i, row in listings.iterrows():
        season_course_ids = new_course_ids.setdefault(row["season_code"], {})
        if row["crn"] in season_course_ids:
            # A previous row has already assigned this course_id
            continue
        crns = row["crns"]
        existing_ids = set(
            course_id_cache.get(f"{row['season_code']}-{crn}") for crn in crns
        )
        # Some listings may be unseen (newly added listings)
        existing_ids.discard(None)
        if len(existing_ids) == 0:
            # None of these CRNs have been seen before, create one.
            new_id = next_course_id
            next_course_id += 1
        else:
            # This either picks the only existing id or the smallest one
            # if there are multiple (i.e. multiple cross-listings merged)
            new_id = min(cast(set[int], existing_ids))
            # Prevent the same course_id being used by another set of CRNs
            # For example, before A and B were cross-listed and had the same
            # course_id; now they are separate, so we need to assign a new
            # course_id for B. We do this by throwing away each course_id once
            # we've assigned it to a set of CRNs.
            for season_crn in course_id_to_listings[new_id]:
                del course_id_cache[season_crn]
        # Invariant: CRNs contain the CRN itself
        if row["crn"] not in crns:
            print(row)
            raise ValueError(f"CRN not in CRNs")
        # Invariant: CRNs form a fully connected component by running DFS
        # Also assign course_ids while we traverse (we use season_course_ids as
        # the visited set)
        stack = [row["crn"]]
        component = []
        adj_list = season_crn_graphs[row["season_code"]]
        num_edges = 0
        while stack:
            v = stack.pop()
            if v not in season_course_ids:
                season_course_ids[v] = new_id
                component.append(v)
                num_edges += len(adj_list[v])
                stack.extend(adj_list[v] - set(season_course_ids))
        # Since each node also has a self-edge, the number of edges should be n^2
        if num_edges != len(component) ** 2:
            print(
                listings[
                    listings["crn"].isin(component)
                    & listings["season_code"].eq(row["season_code"])
                ]
            )
            raise ValueError(f"CRNs not fully connected")

    listings["course_id"] = listings.apply(
        lambda row: new_course_ids[row["season_code"]][row["crn"]], axis=1
    )

    def validate_course_groups(group: pd.DataFrame):
        # Each course has identical sections
        if len(group["section"].unique()) > 1:
            logging.warning(f"Multiple sections for course {group.name}:\n{group}")
        # Each course has distinct course codes
        if len(group["course_code"].unique()) != len(group):
            logging.warning(f"Identical course codes for course {group.name}:\n{group}")

    listings.groupby("course_id").apply(validate_course_groups)
    courses = listings.drop_duplicates(subset="course_id").set_index("course_id")
    return listings, courses


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
            location_data.append(
                {**parse_location(meeting["location"]), "url": meeting["location_url"]}
            )
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
    # convert to JSON string for postgres
    listings["skills"] = listings["skills"].apply(ujson.dumps)
    listings["areas"] = listings["areas"].apply(ujson.dumps)
    listings["section"] = listings["section"].fillna("0").astype(str).replace({"": "0"})
    listings["listing_id"] = generate_id(
        listings,
        lambda row: f"{row['season_code']}-{row['crn']}",
        data_dir / "id_cache" / "listing_id.json",
    )
    listings, courses = resolve_cross_listings(listings, data_dir)
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

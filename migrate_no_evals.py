import pandas as pd

from includes.class_processing import *
from includes.migration import *
from includes.utils import *

import json

"""
================================================================
This script uses the previous CourseTable JSON files and
parsed Yale API files to begin migration of the existing
CourseTable data to the new schema. 

In particular, this script outputs CSV files for the following
tables specified in the schema (docs/2_parsing.md):
    
    - `courses`
    - `listings`
    - `professors`
    - `courses_professors`

Note that the `courses` and `professors` tables are incomplete
because this script does not process the evaluations
================================================================
"""

# load API seasons for post-fall 2014 courses
print("Loading parsed API responses")
with open("api_output/seasons.json", "r") as f:
    parsed_seasons = json.load(f)

# load and merge parsed API outputs
parsed_json = []
for season in parsed_seasons:

    season_courses = pd.read_json(f"api_output/parsed_courses/{season}.json")

    parsed_json.append(season_courses)

parsed_json = pd.concat(parsed_json)
parsed_json = parsed_json.reset_index(drop=True)

# use "<oci_id>_<season>" as temporary course identifiers
parsed_json["listing_id"] = parsed_json["crn"].astype(
    str) + "_" + parsed_json["season_code"].astype(str)
parsed_json = parsed_json.set_index("listing_id")

# load previous course JSON files
print("Loading CourseTable JSON files")
with open("api_output/api_seasons.json", "r") as f:
    all_seasons = json.load(f)

previous_json = []

for season in all_seasons:

    season_courses = pd.read_json(f"api_output/previous_json/{season}.json")
    season_courses["season"] = season

    previous_json.append(season_courses)

previous_json = pd.concat(previous_json)
previous_json = previous_json.reset_index(drop=True)

# use "<oci_id>_<season>" as temporary course identifiers
previous_json["listing_id"] = previous_json["oci_id"].astype(
    str) + "_" + previous_json["season"].astype(str)
previous_json = previous_json.set_index("listing_id")


# initialize `courses` table
print("Constructing `courses` table")
migrated_courses = pd.DataFrame(index=previous_json.index)

migrated_courses["season_code"] = previous_json["season"]

migrated_courses["subject"] = previous_json["subject"]
migrated_courses["number"] = previous_json["number"].astype(str)
migrated_courses["course_code"] = migrated_courses["subject"] + \
    " " + migrated_courses["number"]
migrated_courses["section"] = previous_json["section"]

migrated_courses["title"] = previous_json["long_title"]
migrated_courses["short_title"] = previous_json["title"]

migrated_courses["areas"] = previous_json["areas"]
migrated_courses["skills"] = previous_json["skills"]

migrated_courses["course_home_url"] = previous_json["course_home_url"]
migrated_courses["syllabus_url"] = previous_json["syllabus_url"]

migrated_courses["description"] = previous_json["description"]
migrated_courses["requirements"] = previous_json["requirements"]

migrated_courses["extra_info"] = previous_json["extra_info"]
migrated_courses["flags"] = previous_json["flags"].apply(
    lambda x: extract_flags("".join(x)))

migrated_courses["locations_summary"] = previous_json["locations_summary"]

# migrated_courses["times_long_summary"] = previous_json["times"].apply(
#     lambda x: x.get("long_summary", ""))
# migrated_courses["times_summary"] = previous_json["times"].apply(
#     lambda x: x.get("summary", ""))
# migrated_courses["times_by_day"] = previous_json["times"].apply(
#     lambda x: x.get("by_day", {}))

new_times = previous_json["times"].apply(lambda x: convert_old_meetings(x))

times_summary, times_long_summary, times_by_day = zip(*new_times)

migrated_courses["times_summary"] = times_summary
migrated_courses["times_long_summary"] = times_long_summary
migrated_courses["times_by_day"] = times_by_day

migrated_courses["crn"] = previous_json["oci_id"]
migrated_courses["professors"] = previous_json["professors"]

# add seasons to the cross-listings
print("Formatting and updating cross-listings")


def add_xlist_seasons(row, season_identifier, xlist_identifier):

    season = row[season_identifier]

    listing_ids = [f"{str(x)}_{str(season)}" for x in row[xlist_identifier]]

    return listing_ids


previous_json["oci_ids"] = previous_json.apply(
    add_xlist_seasons, season_identifier="season", xlist_identifier="oci_ids", axis=1)
parsed_json["crns"] = parsed_json.apply(
    add_xlist_seasons, season_identifier="season_code", xlist_identifier="crns", axis=1)

# patch the post-2014 cross-listings with those from the API
previous_json["oci_ids"].update(parsed_json["crns"])

# merge cross-listing info
print("Merging cross-listings")
merged_xlist = previous_json.groupby("season")["oci_ids"].apply(list)
merged_xlist = merged_xlist.apply(lambda x: [set(y) for y in x])

# merge cross-listings from multiple listings, resolve inconsistencies
merged_xlist = merged_xlist.apply(lambda x: merge_overlapping(x))

# format listing table
merged_xlist = merged_xlist.explode().reset_index(drop=True)
merged_xlist = pd.DataFrame(merged_xlist.rename("listing_id"))

merged_xlist["course_id"] = merged_xlist.index
merged_xlist["listing_id"] = merged_xlist["listing_id"].apply(list)
merged_xlist = merged_xlist.explode("listing_id")
merged_xlist = merged_xlist.set_index("listing_id")

migrated_courses["course_id"] = merged_xlist["course_id"]

listing_columns = ["course_id",
                   "subject",
                   "number",
                   "course_code",
                   "section",
                   "season_code",
                   "crn"]

migrated_listings = migrated_courses[listing_columns].copy(deep=True)

migrated_listings.to_csv("migrated_tables/listings.csv")
print("Saved `listings` table")

# construct `professors` table
print("Making `professors` table")
migrated_professors = migrated_courses["professors"].explode().dropna()
migrated_professors = migrated_professors.drop_duplicates(keep="first")
migrated_professors = migrated_professors.reset_index(drop=True)

migrated_professors.name = "name"
migrated_professors.index.name = "professor_id"

migrated_professors = pd.DataFrame(migrated_professors)

migrated_professors.to_csv("migrated_tables/professors_no_evals.csv")
print("Saved `professors` table")

# dictionary mapping for downstream ease
professors_to_ids = dict(
    zip(migrated_professors["name"], migrated_professors.index))

# construct `courses_professors` junction table
print("Making `courses_professors` junction table")

courses_professors = migrated_courses[[
    "professors", "course_id"]].copy(deep=True)

courses_professors = courses_professors.drop_duplicates(
    subset="course_id", keep="first")

courses_professors = courses_professors.explode("professors").dropna()
courses_professors = courses_professors.reset_index(drop=True)

courses_professors["professor_id"] = courses_professors["professors"].map(
    professors_to_ids)
courses_professors = courses_professors.drop("professors", axis=1)

courses_professors.to_csv("migrated_tables/courses_professors.csv")
print("Saved `courses_professors` table")

# deduplicate and finalize `courses` table
print("Deduplicating and finalizing `courses`")
migrated_courses = migrated_courses.drop_duplicates(
    subset="course_id", keep="first")

course_fields = [
    "course_id",
    "season_code",
    "areas",
    "course_home_url",
    "description",
    "extra_info",
    "locations_summary",
    # "num_students",
    # "num_students_is_same_prof",
    "requirements",
    "times_long_summary",
    "times_summary",
    "times_by_day",
    "short_title",
    "skills",
    "syllabus_url",
    "title",
    # "average_overall_rating",
    # "average_workload"
]

migrated_courses = migrated_courses[course_fields]
migrated_courses = migrated_courses.set_index("course_id")

migrated_courses.to_csv("migrated_tables/courses_no_evals.csv")
print("Saved `courses` table")

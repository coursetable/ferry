from pathlib import Path

import pandas as pd
from tqdm import tqdm

from ferry import config
from ferry.includes.embedding_processing import preprocess_description

migrated_courses_files = sorted(list(config.DATA_DIR.glob("migrated_courses/*.json")))
parsed_courses_files = sorted(list(config.DATA_DIR.glob("parsed_courses/*.json")))

parsed_courses_seasons = set([x.stem for x in parsed_courses_files])

# exclude the migrated course files that have a newer parsed version
migrated_courses_files = [
    x for x in migrated_courses_files if x.stem not in parsed_courses_seasons
]

# intermediate list to store course dataframes before concatenation
merged_courses = []

for course_file in migrated_courses_files:
    print(f"Reading in migrated courses for season '{course_file.stem}'")
    courses = pd.read_json(course_file)

    merged_courses.append(courses)

for course_file in parsed_courses_files:
    print(f"Reading in parsed courses for season '{course_file.stem}'")
    courses = pd.read_json(course_file)

    merged_courses.append(courses)

merged_courses = pd.concat(merged_courses, axis=0)

# remove future season courses
merged_courses = merged_courses[merged_courses["season_code"] != 202101]

# remove courses without descriptions
merged_courses = merged_courses[~merged_courses["description"].isna()]
merged_courses = merged_courses[
    ~merged_courses["description"].isin(["", "NA", "n/a", "N/A"])
]

# sort in order of newest first
merged_courses = merged_courses.sort_values(by="season_code", ascending=False)
print(f"Total courses: {len(merged_courses)}")

# drop exact description duplicates
merged_courses = merged_courses.drop_duplicates(subset=["description"], keep="first")
print(f"Total courses (unique descriptions): {len(merged_courses)}")

# drop title duplicates
merged_courses = merged_courses.drop_duplicates(subset=["title"], keep="first")
print(f"Total courses (unique titles): {len(merged_courses)}")

tqdm.pandas(desc="Preprocessing description texts")
merged_courses["preembed_description"] = merged_courses["description"].progress_apply(
    preprocess_description
)

merged_courses.to_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv"
)

with open(config.DATA_DIR / "description_embeddings/descriptions_corpus.txt", "w") as f:
    f.write("\n".join(merged_courses["preembed_description"]) + "\n")

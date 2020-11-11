# pylint: skip-file
from pathlib import Path

import pandas as pd

from ferry import config, database
from ferry.includes.embedding_processing import preprocess_description
from ferry.includes.tqdm import tqdm

print("Reading courses table from database")
courses = pd.read_sql_table("courses", con=database.Engine)

print("Removing courses without descriptions")
# remove courses without descriptions
courses = courses[~courses["description"].isna()]
courses = courses[~courses["description"].isin(["", "NA", "n/a", "N/A"])]

# sort in order of newest first for deduplication (we want to keep the newest ones first)
courses = courses.sort_values(by="season_code", ascending=False)
print(f"Total courses: {len(courses)}")

# drop exact description duplicates
courses = courses.drop_duplicates(subset=["description"], keep="first")
print(f"Total courses (unique descriptions): {len(courses)}")

# drop title duplicates
courses = courses.drop_duplicates(subset=["title"], keep="first")
print(f"Total courses (unique titles): {len(courses)}")

tqdm.pandas(desc="Preprocessing description texts")
courses["title_description"] = courses["title"] + " " + courses["description"]
courses["preembed_description"] = courses["title_description"].progress_apply(
    preprocess_description
)

courses.to_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv"
)

with open(config.DATA_DIR / "description_embeddings/descriptions_corpus.txt", "w") as f:
    f.write("\n".join(courses["preembed_description"]) + "\n")

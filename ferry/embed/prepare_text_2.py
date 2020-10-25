from pathlib import Path

import pandas as pd
from tqdm import tqdm

from ferry import config, database
from ferry.includes.embedding_processing_2 import preprocess_sentences

courses = pd.read_sql_table("courses", con=database.Engine)

# remove future season courses
courses = courses[courses["season_code"] != 202101]

# remove courses without descriptions
courses = courses[~courses["description"].isna()]
courses = courses[~courses["description"].isin(["", "NA", "n/a", "N/A"])]

# sort in order of newest first
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

descriptions = courses["title_description"].tolist()
preembed_descriptions = preprocess_sentences(descriptions)
courses["preembed_description"] = preembed_descriptions
print(courses)


courses.to_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv"
)

with open(config.DATA_DIR / "description_embeddings/descriptions_corpus.txt", "w") as f:
    f.write("\n".join(courses["preembed_description"]) + "\n")

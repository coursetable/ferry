# pylint: skip-file
from pathlib import Path

import pandas as pd

from ferry import config, database
from ferry.includes.prepare_fasttext import preprocess_fasttext
from ferry.includes.prepare_tfidf import preprocess_tfidf
from ferry.includes.tqdm import tqdm

MIN_DESCRIPTION_LENGTH = 8

print("Reading courses table from database")
courses = pd.read_sql_table("courses", con=database.Engine)

print("Removing courses without descriptions")
# remove courses without descriptions
courses = courses[~courses["description"].isna()]
courses = courses[~courses["description"].isin(["", "NA", "n/a", "N/A"])]
courses = courses[courses["description"].apply(len) >= MIN_DESCRIPTION_LENGTH]

# sort in order of newest first for deduplication (we want to keep the newest ones first)
courses = courses.sort_values(by="season_code", ascending=False)
print(f"Total courses: {len(courses)}")

# drop exact title duplicates
courses = courses.drop_duplicates(subset=["title"], keep="first")
print(f"Total courses (unique titles and descriptions): {len(courses)}")

tqdm.pandas(desc="Preprocessing texts for FastText")
courses["title_description"] = courses["title"] + " " + courses["description"]
courses["prepared_fasttext"] = courses["title_description"].progress_apply(
    preprocess_fasttext
)
print("Preprocessing texts for TF-IDF")
courses["prepared_tfidf"] = preprocess_tfidf(courses["title_description"].tolist())

courses.to_csv(config.DATA_DIR / "course_embeddings/courses_deduplicated.csv")

with open(config.DATA_DIR / "course_embeddings/fasttext_corpus.txt", "w") as f:
    f.write("\n".join(courses["prepared_fasttext"]) + "\n")

with open(config.DATA_DIR / "course_embeddings/tfidf_corpus.txt", "w") as f:
    f.write("\n".join(courses["prepared_tfidf"]) + "\n")

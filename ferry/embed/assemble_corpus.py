"""
Assembles the text corpuses for use by FastText and TF-IDF.

Also outputs list of deduplicated courses (by title).
"""
import pandas as pd

from ferry import config
from ferry.includes.prepare_fasttext import preprocess_fasttext
from ferry.includes.prepare_tfidf import preprocess_tfidf
from ferry.includes.tqdm import tqdm

MIN_DESCRIPTION_LENGTH = 8

print("Reading courses table")
courses = pd.read_csv(config.DATA_DIR / "importer_dumps/courses.csv", index_col=0)

# --------------------------------
# Clean up and deduplicate courses
# --------------------------------

print("Removing courses without descriptions")
# remove courses without descriptions
courses = courses[~courses["description"].isna()]
courses = courses[~courses["description"].isin(["", "NA", "n/a", "N/A"])]
courses = courses[courses["description"].apply(len) >= MIN_DESCRIPTION_LENGTH]

# sort in order of newest first for deduplication (we want to keep the newest ones first)
courses = courses.sort_values(by="season_code", ascending=False)
print(f"Total courses: {len(courses)}")

# drop exact title duplicates
courses = courses.drop_duplicates(subset=["title"], keep="first")  # type: ignore
print(f"Total courses (unique titles and descriptions): {len(courses)}")

# index for mapping courses to embedding vectors
courses["embed_index"] = range(len(courses))

# -------------
# Preprocessing
# -------------

tqdm.pandas(desc="Preprocessing texts for FastText")

courses["title_description"] = courses[["title", "description"]].agg(
    " ".join, axis=1
)  # type: ignore
courses["prepared_fasttext"] = courses["title_description"].progress_apply(  # type: ignore
    preprocess_fasttext
)
print("Preprocessing texts for TF-IDF")
courses["prepared_tfidf"] = preprocess_tfidf(courses["title_description"].tolist())

# -------------
# Write outputs
# -------------

courses.to_csv(config.DATA_DIR / "course_embeddings/courses_deduplicated.csv")

with open(config.DATA_DIR / "course_embeddings/fasttext_corpus.txt", "w") as f:
    f.write("\n".join(courses["prepared_fasttext"]) + "\n")

with open(config.DATA_DIR / "course_embeddings/tfidf_corpus.txt", "w") as f:
    f.write("\n".join(courses["prepared_tfidf"]) + "\n")

"""
Computes similar courses per season via maximum-inner product search for both FastText
and TF-IDF embeddings. Intended to be run immediately after /ferry/transform.py so
foreign keys match.

Outputs the fasttext_similars and tfidf_similars tables into /data/importer_dumps ready
for input into /ferry/stage.py.
"""
import numpy as np
import pandas as pd

from ferry import config
from ferry.includes.similarity_search import get_nearest_neighbors

MAX_NEAREST_NEIGHBORS = 16

print("Loading courses and embeddings")

courses = pd.read_csv(config.DATA_DIR / "importer_dumps/courses.csv", index_col=0)

unique_courses = pd.read_csv(
    config.DATA_DIR / "course_embeddings/courses_deduplicated.csv",
    index_col=0,
)
fasttext_embeddings = pd.read_hdf(
    config.DATA_DIR / "course_embeddings/fasttext_embeddings.h5",
    key="embeddings",
)
tfidf_embeddings = pd.read_hdf(
    config.DATA_DIR / "course_embeddings/tfidf_embeddings.h5",
    key="embeddings",
)

# convert to NumPy arrays
fasttext_embeddings = fasttext_embeddings.values
tfidf_embeddings = tfidf_embeddings.values

# map all courses to embedding indices
course_to_embed_index = dict(
    zip(unique_courses["title"], unique_courses["embed_index"])
)
courses["embed_index"] = courses["title"].apply(course_to_embed_index.get)
courses.dropna(subset=["embed_index"], inplace=True)
courses["embed_index"] = courses["embed_index"].astype(int)

# group embedding indices
courses_by_season = courses.set_index("course_id").groupby("season_code")["embed_index"]

seasons = courses_by_season.groups.keys()

fasttext_similars_ = dict()
tfidf_similars_ = dict()

print("Computing similar courses by season")
for season in seasons:

    print(f"Computing similar courses for season {season}")

    season_courses = courses_by_season.get_group(season)

    # course_ids for database foreign keys
    season_course_ids = season_courses.index.values

    # indices of matched embeddings
    season_embed_indices = season_courses.values

    # embedding vectors for both methods
    season_fasttext = fasttext_embeddings[season_embed_indices]
    season_tfidf = tfidf_embeddings[season_embed_indices]

    # exclude NaN values in TF-IDF embeddings
    season_tfidf_valids = np.all(np.isfinite(season_tfidf), axis=1)

    # get similar courses
    season_fasttext_similars = get_nearest_neighbors(
        season_course_ids, season_fasttext, MAX_NEAREST_NEIGHBORS
    )
    season_tfidf_similars = get_nearest_neighbors(
        season_course_ids[season_tfidf_valids],
        season_tfidf[season_tfidf_valids],
        MAX_NEAREST_NEIGHBORS,
    )

    # update dictionary for season
    fasttext_similars_.update(season_fasttext_similars)
    tfidf_similars_.update(season_tfidf_similars)


print("Aggregating similar courses")
# convert dictionary result to DataFrames
fasttext_similars = pd.Series(fasttext_similars_).apply(list).explode()
tfidf_similars = pd.Series(tfidf_similars_).apply(list).explode()

# extract targets and ranks from column tuples
fasttext_similars_df = pd.DataFrame(
    fasttext_similars.values.tolist(), index=fasttext_similars.index
)
tfidf_similars_df = pd.DataFrame(
    tfidf_similars.values.tolist(), index=tfidf_similars.index
)

# reset the courses index to its own column
fasttext_similars_df.reset_index(drop=False, inplace=True)
tfidf_similars_df.reset_index(drop=False, inplace=True)

# specify column names for database compatibility
fasttext_similars_df.columns = ["source", "target", "rank"]
tfidf_similars_df.columns = ["source", "target", "rank"]

print("Writing output tables")
fasttext_similars_df.to_csv(config.DATA_DIR / "importer_dumps/fasttext_similars.csv")
tfidf_similars_df.to_csv(config.DATA_DIR / "importer_dumps/tfidf_similars.csv")

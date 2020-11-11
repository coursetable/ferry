# pylint: skip-file
import numpy as np
import pandas as pd
import ujson

from ferry import config, database
from ferry.includes.similarity_search import get_nearest_neighbors

MAX_NEAREST_NEIGHBORS = 16

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

# convert to Numpy arrays
fasttext_embeddings = fasttext_embeddings.values
tfidf_embeddings = tfidf_embeddings.values

course_to_embed_index = dict(
    zip(unique_courses["title"], unique_courses["embed_index"])
)
courses["embed_index"] = courses["title"].apply(course_to_embed_index.get)
courses.dropna(subset=["embed_index"], inplace=True)
courses["embed_index"] = courses["embed_index"].astype(int)

courses_by_season = courses.set_index("course_id").groupby("season_code")["embed_index"]

seasons = courses_by_season.groups.keys()

fasttext_similars = dict()
tfidf_similars = dict()

for season in seasons:

    print(f"Computing similar courses for season {season}")

    season_courses = courses_by_season.get_group(season)

    season_course_ids = season_courses.index.values
    season_embed_indices = season_courses.values

    season_fasttext = fasttext_embeddings[season_embed_indices]
    season_tfidf = tfidf_embeddings[season_embed_indices]

    season_fasttext_similars = get_nearest_neighbors(
        season_course_ids, season_fasttext, MAX_NEAREST_NEIGHBORS
    )
    season_tfidf_similars = get_nearest_neighbors(
        season_course_ids, season_tfidf, MAX_NEAREST_NEIGHBORS
    )

    fasttext_similars.update(season_fasttext_similars)
    tfidf_similars.update(season_tfidf_similars)

fasttext_similars = pd.Series(fasttext_similars).apply(list).explode()
tfidf_similars = pd.Series(tfidf_similars).apply(list).explode()

fasttext_similars = pd.DataFrame(fasttext_similars)
tfidf_similars = pd.DataFrame(tfidf_similars)

fasttext_similars.reset_index(drop=False, inplace=True)
tfidf_similars.reset_index(drop=False, inplace=True)

fasttext_similars.columns = ["source", "target"]
tfidf_similars.columns = ["source", "target"]

fasttext_similars.to_csv(config.DATA_DIR / "importer_dumps/fasttext_similars.csv")
tfidf_similars.to_csv(config.DATA_DIR / "importer_dumps/tfidf_similars.csv")

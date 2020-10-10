import numpy as np
import pandas as pd
import ujson
import umap
from sklearn.preprocessing import StandardScaler
from tqdm import tqdm
from annoy import AnnoyIndex

from ferry import config

courses = pd.read_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv",
    index_col=0,
)
embeddings = pd.read_hdf(
    config.DATA_DIR / "description_embeddings/description_embeddings.h5",
    key="embeddings",
)

# subset season
embeddings = embeddings[np.array(courses["season_code"] == 202003)]
courses = courses[courses["season_code"] == 202003]

courses["code_title"] = courses["course_code"] + ": " + courses["title"]

num_courses = len(courses)

# reindex courses
courses.index = range(num_courses)

# normalize embeddings
embeddings = StandardScaler().fit_transform(embeddings)

# initialize annoy index w/ angular distance metric
embed_dim = 100
annoy_index = AnnoyIndex(embed_dim, metric="angular")

# add items to index
for i in tqdm(range(num_courses), desc="Adding embeddings to index"):
    annoy_index.add_item(i, embeddings[i])

# build index
annoy_index.build(n_trees=16, n_jobs=-1)

# number of nearest-neighbors to find
nearest_num = 8

course_similars = []

for i in tqdm(range(num_courses), desc="Finding similar courses"):
    similars = annoy_index.get_nns_by_item(i, nearest_num + 1)

    # remove course itself
    similars = [x for x in similars if x != i]
    course_similars.append(similars)

# pull out course titles
course_similars = [list(courses.loc[x, "code_title"]) for x in course_similars]

courses["similar"] = course_similars

# check a few titles we are interested in
titles_subset = [
    "Great Civilizations of the Ancient World",
    "Ordinary and Partial Differential Equations with Applications",
    "Urban Lab: City Making",
    "Painting Basics",
]

courses_subset = courses[courses["title"].isin(titles_subset)][["title", "similar"]]
courses_subset = courses_subset.set_index("title")

print(ujson.dumps(courses_subset.to_dict(orient="index"), indent=2))

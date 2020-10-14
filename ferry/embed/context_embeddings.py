import numpy as np
import pandas as pd
import umap
from sklearn.preprocessing import StandardScaler

from ferry import config, database

courses = pd.read_sql_table("courses", con=database.Engine)
courses = courses.set_index("course_id")

context_vectors = pd.read_hdf(
    config.DATA_DIR / "description_embeddings/context_vectors.h5",
    key="context_vectors",
)

# sort newest-first and drop title+description duplicates
courses = courses.sort_values(by="season_code", ascending=False)
courses = courses.drop_duplicates(subset=["description"], keep="first")
courses = courses.drop_duplicates(subset=["title"], keep="first")

valid_courses = set(courses.index)

# subset nonduplicate courses
context_vectors = context_vectors[context_vectors.index.isin(valid_courses)]

context_embeddings = StandardScaler().fit_transform(context_vectors.to_numpy())

# cast to float64 before UMAP
context_embeddings = context_embeddings.astype(np.float64)

# apply umap
reducer = umap.UMAP()
umap_embeddings = reducer.fit_transform(context_embeddings)

# convert embeddings to DataFrame
umap_embeddings = pd.DataFrame(
    umap_embeddings, index=context_vectors.index, columns=["umap1", "umap2"]
)

# join embeddings with courses
courses = courses.join(umap_embeddings, how="inner")

courses.to_csv(
    config.DATA_DIR / "description_embeddings/courses_context_embedded_umap.csv"
)

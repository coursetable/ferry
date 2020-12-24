"""
Uses UMAP (https://umap-learn.readthedocs.io/en/latest/index.html) to reduce course
embeddings to two dimensions for visualization.
"""
import pandas as pd
import umap
from sklearn.preprocessing import StandardScaler

from ferry import config

courses = pd.read_csv(
    config.DATA_DIR / "course_embeddings/courses_deduplicated.csv",
    index_col=0,
)

# mypy: ignore-errors
embeddings = pd.read_hdf(
    config.DATA_DIR / "course_embeddings/fasttext_embeddings.h5",
    key="embeddings",
)

embeddings = StandardScaler().fit_transform(embeddings)

reducer = umap.UMAP()
umap_embeddings = reducer.fit_transform(embeddings)

courses["umap1"] = umap_embeddings[:, 0]
courses["umap2"] = umap_embeddings[:, 1]

courses.to_csv(config.DATA_DIR / "course_embeddings/courses_deduplicated_umap.csv")

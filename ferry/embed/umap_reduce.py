import pandas as pd
from sklearn.preprocessing import StandardScaler
import umap

from ferry import config

courses = pd.read_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv",
    index_col=0,
)
embeddings = pd.read_hdf(
    config.DATA_DIR / "description_embeddings/description_embeddings.h5",
    key="embeddings",
)

embeddings = StandardScaler().fit_transform(embeddings)

reducer = umap.UMAP()
umap_embeddings = reducer.fit_transform(embeddings)

courses["umap1"] = umap_embeddings[:, 0]
courses["umap2"] = umap_embeddings[:, 1]

courses.to_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated_umap.csv"
)
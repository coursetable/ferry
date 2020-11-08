# pylint: skip-file
import fasttext
import numpy as np
import pandas as pd

from ferry import config

model = fasttext.load_model(
    str(config.DATA_DIR / "description_embeddings/fasttext_model.bin")
)

courses = pd.read_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv",
    index_col=0,
)

with open(config.DATA_DIR / "description_embeddings/descriptions_corpus.txt", "r") as f:
    preembed_descriptions = list(f.readlines())

    # remove newlines at the end
    preembed_descriptions = [x[:-1] for x in preembed_descriptions]

course_embeddings = np.array(
    [model.get_sentence_vector(x) for x in preembed_descriptions]
)

course_embeddings = pd.DataFrame(
    course_embeddings,
    index=courses.index,
    columns=np.arange(course_embeddings.shape[1]),
)

course_embeddings.to_hdf(
    config.DATA_DIR / "description_embeddings/description_embeddings.h5",
    key="embeddings",
    mode="w",
)

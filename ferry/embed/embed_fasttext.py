"""
Trains and embeds a FastText model on the course texts.
"""
import fasttext
import numpy as np
import pandas as pd

from ferry import config

print("Training FastText model")

model = fasttext.train_unsupervised(
    str(config.DATA_DIR / "course_embeddings/fasttext_corpus.txt"),
    model="skipgram",
    lr=0.1,
    dim=100,
    ws=8,
    epoch=100,
)

model.save_model(str(config.DATA_DIR / "course_embeddings/fasttext_model.bin"))

print("Computing embeddings")

courses = pd.read_csv(
    config.DATA_DIR / "course_embeddings/courses_deduplicated.csv",
    index_col=0,
)

with open(config.DATA_DIR / "course_embeddings/fasttext_corpus.txt", "r") as f:
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
    config.DATA_DIR / "course_embeddings/fasttext_embeddings.h5",
    key="embeddings",
    mode="w",
)

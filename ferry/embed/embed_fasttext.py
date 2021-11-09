"""
Trains and embeds a FastText model on the course texts.
"""
import argparse

import fasttext
import numpy as np
import pandas as pd

from ferry import config

config.init_sentry()

parser = argparse.ArgumentParser(description="")
parser.add_argument(
    "-r",
    "--retrain",
    action="store_true",
    help="include to retrain the model from scratch (otherwise loads the previous model)",
)

args = parser.parse_args()

if args.retrain:

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

else:

    print("Loading FastText model")

    model = fasttext.load_model(
        str(config.DATA_DIR / "course_embeddings/fasttext_model.bin")
    )

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

course_embeddings_df = pd.DataFrame(
    course_embeddings,
    index=courses.index,
    columns=np.arange(course_embeddings.shape[1]),
)

print("Writing embedding outputs")

course_embeddings_df.to_hdf(
    config.DATA_DIR / "course_embeddings/fasttext_embeddings.h5",
    key="embeddings",
    mode="w",
)  # type: ignore

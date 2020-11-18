"""
Trains and embeds a TF_IDF model on the course texts.
"""
import numpy as np
import pandas as pd
from gensim import corpora, models

from ferry import config

EMBED_DIM = 100

with open(config.DATA_DIR / "course_embeddings/tfidf_corpus.txt", "r") as f:
    words = [line.split() for line in f]

dictionary = corpora.Dictionary(words)
num_words = len(dictionary)

# bag-of-words corpus
print("Constructing bag-of-words corpus")
corpus_bow = [dictionary.doc2bow(text) for text in words]

print("Constructing TF_IDF model")
tfidf_model = models.TfidfModel(corpus_bow)
corpus_tfidf = tfidf_model[corpus_bow]

# latent semantic indexing model for retrieval
print("Indexing TF-IDF corpus")
lsi_model = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=EMBED_DIM)
corpus_lsi = lsi_model[corpus_tfidf]

# shape is (num_courses, embed_dim)
course_embeddings = np.zeros((len(words), EMBED_DIM), dtype=np.float64)

for idx, course in enumerate(words):

    embedding = [x[1] for x in corpus_lsi[idx]]

    if len(embedding) == EMBED_DIM:
        course_embeddings[idx] = embedding
    else:
        course_embeddings[idx] = np.nan

print("Writing embedding outputs")
courses = pd.read_csv(
    config.DATA_DIR / "course_embeddings/courses_deduplicated.csv",
    index_col=0,
)

course_embeddings = pd.DataFrame(
    course_embeddings,
    index=courses.index,
    columns=np.arange(course_embeddings.shape[1]),
)

course_embeddings.to_hdf(
    config.DATA_DIR / "course_embeddings/tfidf_embeddings.h5",
    key="embeddings",
    mode="w",
)

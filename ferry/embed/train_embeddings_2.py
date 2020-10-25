import numpy as np
import pandas as pd
from gensim import corpora, models, similarities

from ferry import config

with open(config.DATA_DIR / "description_embeddings/descriptions_corpus.txt", "r") as f:
    words = [line.split() for line in f]

dictionary = corpora.Dictionary(words)
num_words = len(dictionary)

corpus_bow = [dictionary.doc2bow(text) for text in words]

tfidf_model = models.TfidfModel(corpus_bow)
corpus_tfidf = tfidf_model[corpus_bow]

lsi_model = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=100)
corpus_lsi = lsi_model[corpus_tfidf]

course_embeddings = np.array(
    [np.array([x[1] for x in corpus_lsi[i]]) for i, course in enumerate(words)]
)

courses = pd.read_csv(
    config.DATA_DIR / "description_embeddings/courses_description_deduplicated.csv",
    index_col=0,
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
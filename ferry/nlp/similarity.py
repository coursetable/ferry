import numpy as np
from gensim import corpora, models, similarities

from load_json import get_all_courses, get_current_courses, get_columns
from preprocess import preprocess_sentences, preprocess_sentences_simple

"""MODEL CREATION"""

create = False

if create:
    # loads data
    data = get_all_courses()
    N = len(data)  # gets number of courses
    print("Courses:", N)

    columns = get_columns(data)
    processed = preprocess_sentences(columns["descriptions"])
    dictionary = corpora.Dictionary(processed)
    num_words = len(dictionary)
    print("Words:", num_words)

    corpus_bow = [dictionary.doc2bow(text) for text in processed]

    tfidf_model = models.TfidfModel(corpus_bow)
    corpus_tfidf = tfidf_model[corpus_bow]

    lsi_model = models.LsiModel(corpus_tfidf, id2word=dictionary, num_topics=100)
    corpus_lsi = lsi_model[corpus_tfidf]

    index = similarities.MatrixSimilarity(corpus_lsi, num_features=num_words)

    dictionary.save("./models/dict.txt")
    tfidf_model.save("./models/tfidf.txt")
    lsi_model.save("./models/lsi.txt")


"""HELPER FUNCTIONS"""


def diff_func(n1, n2, denom):
    return 1.1 - np.log(max(max(n1, n2), 1) / max(min(n1, n2), 1)) / denom


def enrollment_coeff(enrollments, c1, c2):
    e1, e2 = enrollments[c1], enrollments[c2]
    return diff_func(e1, e2, 15)


def listing_coeff(listings, c1, c2):
    l1 = [x["subject"] for x in listings[c1]]
    l2 = [x["subject"] for x in listings[c2]]
    return 1.1 if len(set(l1).intersection(set(l2))) > 0 else 1.0


def number_coeff(listings, c1, c2):
    n1s = [x["number"] for x in listings[c1]]
    n2s = [x["number"] for x in listings[c2]]
    p1 = max([diff_func(int(n1[:3]), int(n2[:3]), 10) for n1 in n1s for n2 in n2s])
    p2 = 1.1 if "L" in "".join(n1s) and "L" in "".join(n2s) else 1.0  # lab courses
    return p1 * p2


def calc_score(enrollments, listings, c1, c2, similarity):
    score = (
        enrollment_coeff(enrollments, c1, c2)
        * listing_coeff(listings, c1, c2)
        * number_coeff(listings, c1, c2)
        * similarity
    )
    return min(score, 1)


def query(data, course_num):
    query_document = processed_curr[course_num]
    query_bow = dictionary.doc2bow(query_document)  # old dictionary
    sims = index_curr[lsi_model[query_bow]]  # old model, but new index
    enrollments = data["enrollments"]
    listings = data["listings"]

    for i, sim in enumerate(sims):
        sims[i] = calc_score(enrollments, listings, course_num, i, sim)

    top3 = sorted(enumerate(sims), key=lambda x: x[1], reverse=True,)[:4]
    return [x for x in top3 if x[1] > 0.5 and x[0] != course_num]


"""MODEL USAGE"""

dictionary = corpora.Dictionary.load("./models/dict.txt")
tfidf_model = models.TfidfModel.load("./models/tfidf.txt")
lsi_model = models.LsiModel.load("./models/lsi.txt")
num_words = len(dictionary)

data_curr = get_current_courses()
columns_curr = get_columns(data_curr)
N = len(columns_curr["titles"])
print(N)

processed_curr = preprocess_sentences(columns_curr["descriptions"])

# uses old dictionary to preserve all words, which model requires
corpus_bow_curr = [dictionary.doc2bow(text) for text in processed_curr]
corpus_tfidf_curr = tfidf_model[corpus_bow_curr]  # old model
corpus_lsi_curr = lsi_model[corpus_tfidf_curr]  # old model

# again, old number of words
index_curr = similarities.MatrixSimilarity(corpus_lsi_curr, num_features=num_words)

for course in range(0, 500, 10):
    title = " ".join(columns_curr["titles"][course])
    print("Searching for courses similar to", title)
    for i, score in query(columns_curr, course):
        title = " ".join(columns_curr["titles"][i])
        print("(" + str(round(score, 4)) + ") " + title)
    print()

similar_courses = {}
for course in range(N):
    similar = query(columns_curr, course)
    for listing in columns_curr["listings"][course]:
        course = listing["subject"] + " " + listing["number"]
        similar_courses[course] = similar

lookup = ["MATH 244", "ENAS 194", "PHYS 260"]

for course in lookup:
    print("Searching for courses similar to", course)
    for c, score in similar_courses[course]:
        title = " ".join(columns_curr["titles"][c])
        print("(" + str(round(score, 4)) + ")" + title)
    print()

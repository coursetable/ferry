from collections import defaultdict

import spacy

from load_json import get_columns, get_current_courses
from preprocess import clean_keyword as clean

# "YC401": "What knowledge, skills, and insights did you develop by taking this course?"
# "YC403": "What are the strengths and weaknesses of this course and how could it be improved?"
# "YC409": "Would you recommend this course to another student? Please explain."

nlp = spacy.load("en_core_web_sm")

data = get_current_courses()
columns = get_columns(data)
titles = [" ".join(x) for x in columns["titles"]]
descriptions = columns["descriptions"]
evaluations = columns["evaluations"]
N = len(titles)

for i in range(N):
    keywords = defaultdict(lambda: defaultdict(int))
    doc = nlp(descriptions[i])
    for chunk in doc.noun_chunks:
        keyword, original = clean(chunk, titles[i])
        if len(keyword) > 0:
            keywords[keyword][original] += 1

    evals = evaluations[i]["YC401"]
    for eval in evals:
        doc = nlp(eval)
        for chunk in doc.noun_chunks:
            keyword, original = clean(chunk, titles[i])
            if len(keyword) > 0:
                keywords[keyword][original] += 1

    counts = {
        sorted(v.items(), key=lambda x: x[1], reverse=True)[0][0]: sum(v.values())
        for k, v in keywords.items()
    }
    counts = {k: counts[k] for k in counts if counts[k] > 3}
    counts = sorted(counts.items(), key=lambda x: (x[1], len(x[0])), reverse=True)
    new_counts = []
    for count in counts:
        # if not a substring of a more common keyword, include
        if not any([count[0] in x[0] for x in new_counts]):
            new_counts.append(count)
    counts = new_counts[: min(5, len(counts))]

    if len(counts) > 1:
        print(titles[i])
        print(descriptions[i])
        print(counts)
        print("\n\n\n")

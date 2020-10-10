import spacy
import pytextrank
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
import nltk.data

from load_json import get_all_courses, get_current_courses, get_columns

# "YC401": "What knowledge, skills, and insights did you develop by taking this course?"
# "YC403": "What are the strengths and weaknesses of this course and how could it be improved?"
# "YC409": "Would you recommend this course to another student? Please explain."

sent_detector = nltk.data.load("tokenizers/punkt/english.pickle")

# load a spaCy model, depending on language, scale, etc.
nlp = spacy.load("en_core_web_sm")

# add PyTextRank to the spaCy pipeline
tr = pytextrank.TextRank()
nlp.add_pipe(tr.PipelineComponent, name="textrank", last=True)

analyzer = SentimentIntensityAnalyzer()


data = get_current_courses()
columns = get_columns(data)
titles = [" ".join(x) for x in columns["titles"]]
evaluations = columns["evaluations"]
N = len(titles)


def summarize_course(text):
    replaces = [
        ["\n", " "],
        ["\r", " "],
        ["-", " "],
        ["!.", "! "],
        ["?.", "? "],
        ["...", ".."],
        ["..", ". "],
        [" .", ". "],
        ["  ", " "],
    ]
    for replace in replaces:
        while replace[0] in text:
            text = text.replace(replace[0], replace[1])

    if len(text) == 0:
        return []

    # to help with sentence detector
    text = (
        text.replace("Strength:", ". Strength:")
        .replace("Strengths:", ". Strengths:")
        .replace("strength:", ". Strength: ")
        .replace("strengths:", ". Strengths: ")
    )
    text = (
        text.replace("Weakness:", ". Weakness: ")
        .replace("Weaknesses:", ". Weaknesses: ")
        .replace("weakness:", ". Weakness: ")
        .replace("weaknesses:", ". Weaknesses: ")
    )
    text = (
        text.replace("Improvement:", ". Improvement: ")
        .replace("Improvements:", ". Improvements: ")
        .replace("improvement:", ". Improvement: ")
        .replace("improvements:", ". Improvements: ")
    )

    text = [str(t) for t in sent_detector.tokenize(text) if len(str(t)) > 50]

    for i in range(len(text)):
        while text[i][0] in ".:- ":
            text[i] = text[i][1:]
    text = " ".join(text)

    # sends to spacy
    doc = nlp(text)

    dict = {}
    counts = [1, 2, 1]  # capacities for positive, neutral, negative feedback
    sentences = doc._.textrank.summary(limit_phrases=30, limit_sentences=20)
    sentences = [str(sent) for sent in sentences]  # initially not a string
    scores = [analyzer.polarity_scores(str(sent))["compound"] for sent in sentences]

    # updates after sentiment calculated
    removes = [
        ["But ", ""],
        ["But, ", ""],
        ["Another ", "One "],
        ["Also ", ""],
        ["Also, ", ""],
        ["That said, ", ""],
    ]
    for i, sent in enumerate(sentences):
        sent = sent.upper()[0] + sent[1:]
        # sent = sent.replace("Strength: ", "").replace("Strengths: ", "")
        # sent = sent.replace("Weakness: ", "").replace("Weaknesses: ", "")
        for remove in removes:
            if sent[: len(remove[0])] == remove[0]:
                sent = sent[len(remove[0]) :] + remove[1]
        sent.replace("also", "").replace("Also", "")
        sent = sent.upper()[0] + sent[1:]
        sentences[i] = sent

    bounds = [50, 150]
    # initially restricts to 2 per category, greedy approach
    for i, sent in enumerate(sentences):
        if sum(counts) > 0 and bounds[0] < len(sent) < bounds[1]:
            score = scores[i]
            if score < -0.25 and counts[0] > 0:
                counts[0] -= 1
                dict[sent] = score
            elif -0.25 < score < 0.75 and counts[1] > 0:
                counts[1] -= 1
                dict[sent] = score
            elif score > 0.75 and counts[2] > 0:
                counts[2] -= 1
                dict[sent] = score

    # if any not filled, second pass attempts to fill with anything
    count = sum(counts)
    for i, sent in enumerate(sentences):
        if count > 0 and bounds[0] < len(sent) < bounds[1]:
            if sent not in dict:
                dict[sent] = scores[i]
                count -= 1

    sorted_sentences = sorted(dict.items(), key=lambda x: x[1], reverse=True)
    return sorted_sentences


# example text
for i in range(N):
    text = ". ".join(evaluations[i]["YC403"])
    sentences = summarize_course(text)
    print(titles[i])
    for i, (sent, score) in enumerate(sentences):
        print(i + 1, sent)
    print()

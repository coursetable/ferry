import string
from collections import Counter

from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag

import enchant
from gensim.models.phrases import Phraser, Phrases

punc = string.punctuation + "“" + "”" + "1234567890"
sw = stopwords.words("english")

common = ["students", "course", "study", "topics", "term", "skills", "studies"]
common += ["emphasis", "materials", "faculty", "including", "use", "section"]
common += ["work", "must", "well", "focus", "practice", "class", "first", "take"]
common += ["may", "undergraduate", "works", "proficiency", "student", "one", "include"]
common += ["required", "week", "also", "two", "yale", "deadline", "development"]
common += ["analysis", "design", "system", "see", "term", "enrollment", "required"]
common += ["take", "cpsc", "monday", "tuesday", "wednesday", "thursday", "friday"]
common += ["instructor", "dus", "particular", "take", "way", "year", "lot", "topic"]
common += ["dean", "something", "end", "professor"]

combined = sw + common

# only exclude for keyword extraction
keyword_exclude = ["introduction", "advanced", "basic", "field", "knowledge", "concept"]
keyword_exclude += ["insight", "skill", "various", "intro", "paper", "thing", "problem"]
keyword_exclude += ["lecture", "process", "high", "school", "understanding", "final"]
keyword_exclude += ["project", "understand", "everything"]

# fix keyword plurality after lemmatize
make_plural = ["mechanic", "wave"]

wordnet_map = {"N": wordnet.NOUN, "V": wordnet.VERB, "J": wordnet.ADJ, "R": wordnet.ADV}
lemmatizer = WordNetLemmatizer()

d = enchant.Dict("en_US")
found = {}


def remove_punc(sentences):
    # removes common punctuation
    return [
        [
            "".join([c for c in word if c.isalpha()])
            for word in sentence
            if len(word) > 0
        ]
        for sentence in sentences
    ]


def remove_stopwords(sentences):
    return [
        [word for word in sentence if word not in sw and len(word) > 0]
        for sentence in sentences
    ]


def remove_common(sentences):
    return [
        [word for word in sentence if word not in common and len(word) > 0]
        for sentence in sentences
    ]


def remove_small(sentences):
    return [[word for word in sentence if len(word) > 2] for sentence in sentences]


def remove_rare(sentences):
    words = []
    for sentence in sentences:
        for word in sentence:
            words.append(word)
    counts = Counter(words)
    return [[word for word in sentence if counts[word] > 1] for sentence in sentences]


def lemmatize_words(text):
    if len(text) == 0:
        return []
    pos_tagged_text = pos_tag(text)
    return [
        lemmatizer.lemmatize(word, wordnet_map.get(pos[0], wordnet.NOUN))
        for word, pos in pos_tagged_text
    ]


def lemmatize_sentences(sentences):
    return [lemmatize_words(desc) for desc in sentences]


def phrase_sentences(sentences):
    phrases = Phrases(sentences, threshold=0.5, min_count=2, scoring="npmi")
    bigram = Phraser(phrases)
    return list(bigram[sentences])


def spellcheck_words(text):
    out = []
    for i in range(len(text)):
        if text[i] in found:
            out.append(text[i])
        elif d.check(text[i]):
            found[text[i]] = True
        elif not d.check(text[i]):
            options = d.suggest(text[i])
            if len(options) > 0:
                out.append(options[0])
    return text


def spellcheck_sentences(sentences):
    return [spellcheck_words(sentence) for sentence in sentences]


def preprocess_sentences(sentences, rare=True):
    sentences = [
        " ".join(sentence.lower().split("-")).split(" ") for sentence in sentences
    ]
    sentences = remove_punc(sentences)
    sentences = remove_stopwords(sentences)
    sentences = remove_common(sentences)
    sentences = remove_small(sentences)
    if rare:
        sentences = remove_rare(sentences)
    sentences = lemmatize_sentences(sentences)
    return sentences


# does not apply common, rare, lemmatize
def preprocess_sentences_simple(sentences):
    sentences = [
        " ".join(sentence.lower().split("-")).split(" ") for sentence in sentences
    ]
    sentences = remove_punc(sentences)
    sentences = remove_stopwords(sentences)
    sentences = remove_small(sentences)
    return sentences


def clean_keyword(chunk, title):
    title_exclude = preprocess_sentences([title], rare=False)[0]
    exclude = combined + keyword_exclude + title_exclude
    text = " ".join(chunk.text.lower().split("-")).split(" ")

    text = remove_punc([text])[0]  # hack for single input
    text = remove_small([text])[0]

    text = ["biology" if t == "bio" else t for t in text]
    original = text  # pre-lemmatize

    # lemmatize process
    text = lemmatize_words(text)  # standardizes words
    text = remove_small([text])[0]  # after lemmatize

    while len(text) > 0 and text[0] in exclude:
        text = text[1:]

    while len(original) > 0 and original[0] in exclude:
        original = original[1:]

    if len(text) == 0 or len(original) == 0:
        return "", ""

    return " ".join(text), " ".join(original)

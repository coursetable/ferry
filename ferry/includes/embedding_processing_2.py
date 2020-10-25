import string
from collections import Counter

from gensim.models.phrases import Phraser, Phrases
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag

from unidecode import unidecode

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

wordnet_map = {"N": wordnet.NOUN, "V": wordnet.VERB, "J": wordnet.ADJ, "R": wordnet.ADV}
lemmatizer = WordNetLemmatizer()


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


def preprocess_sentences(sentences, rare=True):
    sentences = [
        " ".join(sentence.lower().split("-")).split(" ") for sentence in sentences
    ]
    sentences = [[unidecode(word) for word in sentence] for sentence in sentences]
    sentences = remove_punc(sentences)
    sentences = remove_stopwords(sentences)
    sentences = remove_common(sentences)
    sentences = remove_small(sentences)
    if rare:
        sentences = remove_rare(sentences)
    sentences = lemmatize_sentences(sentences)
    return [" ".join(sentence) for sentence in sentences]

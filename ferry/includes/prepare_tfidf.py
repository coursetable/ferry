# pylint: skip-file
import string
from collections import Counter
from typing import Any, List

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


def remove_punc(sentences: List[List[str]]) -> List[List[str]]:
    # removes common punctuation
    return [
        [
            "".join([c for c in word if c.isalpha()])
            for word in sentence
            if len(word) > 0
        ]
        for sentence in sentences
    ]


def remove_stopwords(sentences: List[List[str]]) -> List[List[str]]:
    return [
        [word for word in sentence if word not in sw and len(word) > 0]
        for sentence in sentences
    ]


def remove_common(sentences: List[List[str]]) -> List[List[str]]:
    return [
        [word for word in sentence if word not in common and len(word) > 0]
        for sentence in sentences
    ]


def remove_small(sentences: List[List[str]]) -> List[List[str]]:
    return [[word for word in sentence if len(word) > 2] for sentence in sentences]


def remove_rare(sentences: List[List[str]]) -> List[List[str]]:
    counts: Counter = Counter()
    for sentence in sentences:
        counts.update(sentence)
    return [[word for word in sentence if counts[word] > 1] for sentence in sentences]


def lemmatize_words(text: List[str]) -> List[Any]:
    if len(text) == 0:
        return []
    pos_tagged_text = pos_tag(text)
    return [
        lemmatizer.lemmatize(word, wordnet_map.get(pos[0], wordnet.NOUN))
        for word, pos in pos_tagged_text
    ]


def lemmatize_sentences(sentences: List[List[str]]) -> List[List[str]]:
    return [lemmatize_words(desc) for desc in sentences]


def preprocess_tfidf(sentences: List[str], rare=True) -> List[str]:
    sentences_split = [sentence.lower().split("-") for sentence in sentences]
    sentences_split = [
        [unidecode(word) for word in sentence] for sentence in sentences_split
    ]
    sentences_split = remove_punc(sentences_split)
    sentences_split = remove_stopwords(sentences_split)
    sentences_split = remove_common(sentences_split)
    sentences_split = remove_small(sentences_split)
    if rare:
        sentences_split = remove_rare(sentences_split)
    sentences_split = lemmatize_sentences(sentences_split)
    return [" ".join(sentence) for sentence in sentences_split]

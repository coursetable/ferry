"""
Preprocess text for TF-IDF embeddings.

Used by /ferry/embed/assemble_corpus.py.
"""
from collections import Counter
from typing import Any, List

from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.tag import pos_tag
from unidecode import unidecode

STOP_WORDS = set(stopwords.words("english"))

# super common words to ignore
COMMON_WORDS = {
    "students",
    "course",
    "study",
    "topics",
    "term",
    "skills",
    "studies",
    "emphasis",
    "materials",
    "faculty",
    "including",
    "use",
    "section",
    "work",
    "must",
    "well",
    "focus",
    "practice",
    "class",
    "first",
    "take",
    "may",
    "undergraduate",
    "works",
    "proficiency",
    "student",
    "one",
    "include",
    "required",
    "week",
    "also",
    "two",
    "yale",
    "deadline",
    "development",
    "analysis",
    "design",
    "system",
    "see",
    "term",
    "enrollment",
    "required",
    "take",
    "cpsc",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "instructor",
    "dus",
    "particular",
    "take",
    "way",
    "year",
    "lot",
    "topic",
    "dean",
    "something",
    "end",
    "professor",
}


WORDNET_MAP = {"N": wordnet.NOUN, "V": wordnet.VERB, "J": wordnet.ADJ, "R": wordnet.ADV}
LEMMATIZER = WordNetLemmatizer()


def remove_punc(sentences: List[List[str]]) -> List[List[str]]:
    """
    Remove punctuation from sentences.

    Parameters
    ----------
    sentences:
        List of tokenized sentences.
    """
    return [
        [
            "".join([c for c in word if c.isalpha()])
            for word in sentence
            if len(word) > 0
        ]
        for sentence in sentences
    ]


def remove_stopwords(sentences: List[List[str]]) -> List[List[str]]:
    """
    Remove stop words from sentences.

    Parameters
    ----------
    sentences:
        List of tokenized sentences.
    """
    return [
        [word for word in sentence if word not in STOP_WORDS and len(word) > 0]
        for sentence in sentences
    ]


def remove_common(sentences: List[List[str]]) -> List[List[str]]:
    """
    Remove common words from sentences.

    Parameters
    ----------
    sentences:
        List of tokenized sentences.
    """
    return [
        [word for word in sentence if word not in COMMON_WORDS and len(word) > 0]
        for sentence in sentences
    ]


def remove_small(sentences: List[List[str]]) -> List[List[str]]:
    """
    Remove small words (2 letters or less) from sentences.

    Parameters
    ----------
    sentences:
        List of tokenized sentences.
    """
    return [[word for word in sentence if len(word) > 2] for sentence in sentences]


def remove_rare(sentences: List[List[str]]) -> List[List[str]]:
    """
    Remove rare words (those that appear at most once) from sentences.

    Parameters
    ----------
    sentences:
        List of tokenized sentences.
    """
    counts: Counter = Counter()
    for sentence in sentences:
        counts.update(sentence)
    return [[word for word in sentence if counts[word] > 1] for sentence in sentences]


def lemmatize_words(text: List[str]) -> List[Any]:
    """
    Lemmatize words in a sentence.

    Parameters
    ----------
    text:
        Tokenized sentence.
    """
    if len(text) == 0:
        return []
    pos_tagged_text = pos_tag(text)
    return [
        LEMMATIZER.lemmatize(word, WORDNET_MAP.get(pos[0], wordnet.NOUN))
        for word, pos in pos_tagged_text
    ]


def lemmatize_sentences(sentences: List[List[str]]) -> List[List[str]]:
    """
    Lemmatize sentences.

    Parameters
    ----------
    sentences:
        List of tokenized sentences.
    """
    return [lemmatize_words(desc) for desc in sentences]


def preprocess_tfidf(sentences: List[str], exclude_rare=True) -> List[str]:
    """
    Prepare sentences for TF-IDF embedding.

    Parameters
    ----------
    sentences:
        List of raw sentences.

    exclude_rare:
        Whether to exclude rare words.
    """
    sentences_split = [sentence.lower().split("-") for sentence in sentences]
    sentences_split = [
        [unidecode(word) for word in sentence] for sentence in sentences_split
    ]
    sentences_split = remove_punc(sentences_split)
    sentences_split = remove_stopwords(sentences_split)
    sentences_split = remove_common(sentences_split)
    sentences_split = remove_small(sentences_split)

    if exclude_rare:
        sentences_split = remove_rare(sentences_split)

    sentences_split = lemmatize_sentences(sentences_split)
    return [" ".join(sentence) for sentence in sentences_split]

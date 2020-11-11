# pylint: skip-file
import re

import nltk
import spacy
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.tokenize import sent_tokenize, word_tokenize

nltk.download("averaged_perceptron_tagger")
nltk.download("wordnet")
nltk.download("punkt")
nltk.download("stopwords")

spacy_en = spacy.load("en_core_web_sm", disable=["parser", "ner"])


def remove_punctuation(text):
    """
    Remove non-alphanumeric characters from a text, replacing them with spaces.

    Parameters
    ----------
    text: string
        Text to process

    Returns
    -------
    text
    """

    # replace non-alphanumeric symbols with spaces
    pattern = r"[^a-zA-Z0-9\s]"
    text = re.sub(pattern, " ", text)

    # collapse to single spaces and strip start/end spaces
    text = re.sub(r"\s+", " ", text).strip()

    return text


def collapse_numbers(text):

    """
    Collapse consecutive numeric characters to a single '#' symbol

    Parameters
    ----------
    text: string
        Text to process

    Returns
    -------
    text
    """

    if bool(re.search(r"\d", text)):
        text = re.sub("(^|\s)[0-9]+", "#", text)
    return text


stop_words = set(stopwords.words("english"))


def remove_stop_words(text):
    """
    Remove stop words from a list of tokens.

    Parameters
    ----------
    text: list-like
        Text to process

    Returns
    -------
    text
    """
    return [x for x in text if x not in stop_words]


def lemmatize(text):
    """
    Lemmatize tokens in a list

    Parameters
    ----------
    text: list-like
        Text to process

    Returns
    -------
    text
    """
    text = spacy_en(text)

    return [x.lemma_ for x in text]


def preprocess_description(description):
    """
    Preprocess a course description for downstream embedding training

    Parameters
    ----------
    description: string
        Description text to process

    Returns
    -------
    text
    """

    description = description.lower()
    description = description.replace("\n", " ").replace("\r", "")

    description = remove_punctuation(description)
    description = collapse_numbers(description)

    description = word_tokenize(description)
    description = remove_stop_words(description)

    description = " ".join(description)
    description = lemmatize(description)
    description = " ".join(description)

    return description

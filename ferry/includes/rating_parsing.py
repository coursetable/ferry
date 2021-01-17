"""
Functions for processing course rating JSONs into aggregate CSVs.

Used by /ferry/crawler/parse_ratings.py
"""
import csv
from typing import Any, Dict

import ujson
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# initialize sentiment intensity analyzer
analyzer = SentimentIntensityAnalyzer()


def process_narratives(evaluation: Dict[str, Any], narratives_writer: csv.DictWriter):
    """
    Process written evaluations. Appends to narratives CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    narratives_writer:
        CSV writer to narratives output file.
    """
    for narrative_group in evaluation["narratives"]:

        for raw_narrative in narrative_group["comments"]:

            narrative = {}
            narrative["season"] = evaluation["season"]
            narrative["crn"] = evaluation["crn_code"]
            narrative["question_code"] = narrative_group["question_id"]
            narrative["comment"] = raw_narrative

            sentiment = analyzer.polarity_scores(raw_narrative)

            narrative["comment_neg"] = sentiment["neg"]
            narrative["comment_neu"] = sentiment["neu"]
            narrative["comment_pos"] = sentiment["pos"]
            narrative["comment_compound"] = sentiment["compound"]

            narratives_writer.writerow(narrative)


def process_ratings(evaluation: Dict[str, Any], ratings_writer: csv.DictWriter):
    """
    Process categorical evaluations. Appends to ratings CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    ratings_writer:
        CSV writer to ratings output file.
    """
    for raw_rating in evaluation["ratings"]:
        rating = {}

        rating["season"] = evaluation["season"]
        rating["crn"] = evaluation["crn_code"]
        rating["question_code"] = raw_rating["question_id"]
        rating["rating"] = ujson.dumps(raw_rating["data"])

        ratings_writer.writerow(rating)


def process_statistics(evaluation: Dict[str, Any], statistics_writer: csv.DictWriter):
    """
    Process evaluation statistics. Appends to statistics CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    statistics_writer:
        CSV writer to course statistics output file.
    """
    statistics = {}
    statistics["season"] = evaluation["season"]
    statistics["crn"] = evaluation["crn_code"]
    statistics["enrolled"] = evaluation["enrollment"]["enrolled"]
    statistics["responses"] = evaluation["enrollment"]["responses"]
    statistics["declined"] = evaluation["enrollment"]["declined"]
    statistics["no_response"] = evaluation["enrollment"]["no response"]
    statistics["extras"] = evaluation["extras"]

    statistics_writer.writerow(statistics)


def process_questions(evaluation: Dict[str, Any], questions_writer: csv.DictWriter):
    """
    Process evaluation questions. Appends to questions CSV with global writer object.

    Parameters
    ----------
    evaluation:
        evaluation object for a course.
    questions_writer:
        CSV writer to questions output file.
    """
    for rating in evaluation["ratings"]:
        question = {}
        question["season"] = evaluation["season"]
        question["crn"] = evaluation["crn_code"]
        question["question_code"] = rating["question_id"]
        question["question_text"] = rating["question_text"]
        question["is_narrative"] = False
        question["options"] = ujson.dumps(rating["options"])

        questions_writer.writerow(question)

    for narrative in evaluation["narratives"]:

        question = {}
        question["season"] = evaluation["season"]
        question["crn"] = evaluation["crn_code"]
        question["question_code"] = narrative["question_id"]
        question["question_text"] = narrative["question_text"]
        question["is_narrative"] = True
        question["options"] = None

        questions_writer.writerow(question)

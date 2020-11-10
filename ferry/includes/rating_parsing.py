"""
Functions for use by /ferry/crawler/parse_ratings.py
"""

import ujson
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# initialize sentiment intensity analyzer
analyzer = SentimentIntensityAnalyzer()


def process_narratives(evaluation, narratives_writer):
    """
    Process written evaluations. Appends to narratives CSV with
    global writer object.
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


def process_ratings(evaluation, ratings_writer):
    """
    Process categorical evaluations. Appends to ratings CSV with
    global writer object.
    """

    for raw_rating in evaluation["ratings"]:
        rating = {}

        rating["season"] = evaluation["season"]
        rating["crn"] = evaluation["crn_code"]
        rating["question_code"] = raw_rating["question_id"]
        rating["rating"] = ujson.dumps(raw_rating["data"])

        ratings_writer.writerow(rating)


def process_statistics(evaluation, statistics_writer):
    """
    Process evaluation statistics. Appends to statistics CSV with
    global writer object.
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


def process_questions(evaluation, questions_writer):
    """
    Process evaluation questions. Appends to questions CSV with
    global writer object.
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
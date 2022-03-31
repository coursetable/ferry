"""
Functions for processing ratings.

Used by /ferry/crawler/fetch_ratings.py.
"""
import time
from typing import Any, Dict, List, Tuple

import requests
import ujson
from bs4 import BeautifulSoup

from ferry import config

QuestionId = str


class _EvaluationsNotViewableError(Exception):
    """
    Object for inaccessible evaluations exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class CrawlerError(Exception):
    """
    Object for crawler exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


def fetch_questions(
    session: requests.Session, crn: str, term_code: str
) -> Dict[QuestionId, str]:
    """
    Get list of question Ids for a certain course from OCE.

    Parameters
    ----------
    session:
        Current session with login cookie.
    crn:
        CRN of the course
    term_code:
        Term code that the course belongs to.
    Returns
    -------
    questions:
        Map from question IDs to question text.
    """

    if data_show["minEnrollment"] == "N":
        raise _EvaluationsNotViewableError("No minimum enrollment to view.")
    if data_show["minCompleted"] == "N":
        raise _EvaluationsNotViewableError("No minimum evaluations completed to view.")
    if data_show["gradesSubmitted"] == "N":
        raise CrawlerError(
            "These evaluations are not viewable. Not all grades have been submitted."
        )

    question_list = data_show["questionList"]

    questions = {}
    for question in question_list:
        question_id = question["questionId"]
        text = question["text"]
        # Strip out
        #   "
        #    <br/> \r\n<br/><i>(Your anonymous response to this question may
        #   be viewed by Yale College students, faculty, and advisers to aid
        #   in course selection and evaluating teaching.)</i>
        #   "
        text = text[0 : text.find("<br/>")]

        questions[question_id] = text

    if len(questions) == 0:  # Evaluation data for this course not available
        raise CrawlerError(
            f"Evaluations for course crn={crn} in term={term_code} are unavailable"
        )

    return questions


def fetch_eval_data(page, question_id) -> Tuple[List[int], List[str]]:
    """
    Get rating data for each question of a course.

    Parameters
    ----------
    session:
        Current session with login cookie.
    question_id:
        questionId to fetch evaluation data for.
    crn:
        CRN of the course.
    term_code:
        Term code that the course belongs to.
    Returns
    -------
    ratings, options:
        Evaluation data for the question ID, and the response options.
    """

    soup = BeautifulSoup(page.content, "lxml")

    table = soup.find("table", id="answers" + str(question_id))

    rows = table.findChildren("tr")

    ratings = []
    options = []
    for row in rows:
        item = row.findChildren("td")
        ratings.append(item[1])
        options.append(item[0])

    return ratings, options


def fetch_comments(
    session: requests.Session, offset: int, _max: int, crn: str, term_code: str
) -> Dict[str, Any]:
    """
    Get comments for a specific question of this course.

    Parameters
    ----------
    session:
        Current session with login cookie.
    offset:
        Question to fetch comments for. 0-indexed.
    _max:
        Always 1. Just passed into get function.
    crn:
        CRN of the course.
    term_code:
        Term code that the course belongs to.

    Returns
    -------
    Dictionary of
        {
            question_id
            question_text
            comments
        }
    """
    # Website with student comments for this question
    url_comments = "https://oce.app.yale.edu/oce-viewer/studentComments/index"

    page_comments = session.get(url_comments, params={"offset": offset, "max": _max})

    if page_comments.status_code != 200:
        # Question doesn't exist
        raise CrawlerError("no more evals available")

    soup = BeautifulSoup(page_comments.content, "lxml")

    # save raw HTML in case we ever need it
    with open(
        config.DATA_DIR
        / f"rating_cache/comments/{term_code}_{crn}_{offset}_{_max}.html",
        "w",
    ) as file:
        file.write(str(soup))

    question_html = soup.find(id="cList")

    # Question text.
    question_text = question_html.select_one("div > p:nth-of-type(2)").text
    if question_text is None or question_text == "":
        raise CrawlerError("no more evals available")

    # Question ID.
    info_area = question_html.select_one("div > p:nth-of-type(3)")
    question_id = info_area.contents[1].strip()

    # Responses.
    comments = []  # List of responses
    for answer_area in soup.find_all(id="answer"):
        answer = answer_area.find(id="text").get_text(strip=True)
        comments.append(answer)

    return {
        "question_id": question_id,
        "question_text": question_text,
        "comments": comments,
    }


def fetch_course_enrollment(
    session: requests.Session, crn: str, term_code: str
) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """
    Get enrollment statistics for this course.

    Parameters
    ----------
    session:
        Current session with login cookie
    crn:
        CRN of the course.
    term_code:
        Term code that the course belongs to.
    Returns
    -------
    stats, extras:
        A dictionary with statistics, a dictionary with extra info
    """
    # Main website with number of questions
    url_index = "https://oce.app.yale.edu/oce-viewer/studentSummary/index"

    class_info = {
        "crn": crn,
        "term_code": term_code,
    }
    page_index = session.get(url_index, params=class_info)
    if page_index.status_code != 200:  # Evaluation data for this term not available
        raise CrawlerError("missing enrollment data")

    soup = BeautifulSoup(page_index.content, "lxml")

    stats = {}
    stats_area = soup.find(id="status")
    for item in stats_area.find_all("li"):
        stat = item.p
        if not stat.get_text(strip=True):
            continue
        name = stat.contents[0].strip()[:-1].lower()
        value = int(stat.contents[1].get_text())
        stats[name] = value

    title = (
        stats_area.parent.find(text=" Overview of Evaluations : ")
        .parent.findNext("b")
        .text
    )

    return stats, {"title": title}


def fetch_course_eval(
    session: requests.Session, crn_code: str, term_code: str
) -> Dict[str, Any]:

    """
    Gets evaluation data and comments for the specified course in specified term.

    Parameters
    ----------
    session:
        The current session with login cookie.
    crn_code:
        CRN of this course.
    term_code:
        term code of this course.

    Returns
    -------
    course_eval:
        Dictionary with all evaluation data.
    """

    # print("TERM:",term_code,"CRN CODE:",crn_code)

    # Main website with number of questions
    url_index = "https://oce.app.yale.edu/ocedashboard/studentViewer/courseSummary"

    class_info = {
        "crn": crn_code,
        "termCode": term_code,
    }

    page_index = session.get(url_index, params=class_info)
    if page_index.status_code != 200:  # Evaluation data for this term not available
        raise CrawlerError(f"Evaluations for term {term_code} are unavailable")

    # save raw HTML in case we ever need it
    with open(
        config.DATA_DIR / f"rating_cache/questions_index/{term_code}_{crn}.html", "w"
    ) as file:
        file.write(str(page_index.content))

    # Enrollment data.
    enrollment, extras = fetch_course_enrollment(page_index)

    # Fetch ratings questions.
    try:
        questions = fetch_questions(page_index)
    except _EvaluationsNotViewableError as err:
        questions = {}
        extras["not_viewable"] = str(err)

    # Numeric evaluations data.
    ratings = []
    for question_id, text in questions.items():
        data, options = fetch_eval_data(page_index, question_id)
        ratings.append(
            {
                "question_id": question_id,
                "question_text": text,
                "options": options,
                "data": data,
            }
        )

    # Narrative evaluations data.
    narratives = []
    offset = 0  # Start with first question
    while questions:  # serves as an if + while True
        try:
            # Get questions with their respective responses
            narratives.append(fetch_comments(page, offset, 1))
            offset += 1  # Increment to next question
        except CrawlerError as err:
            if offset == 0:
                raise CrawlerError("cannot fetch narrative comments") from err
            # No more questions are available -- normal situation.
            break

    course_eval: Dict[str, Any] = {}
    course_eval["crn_code"] = crn_code
    course_eval["season"] = term_code
    course_eval["enrollment"] = enrollment
    course_eval["ratings"] = ratings
    course_eval["narratives"] = narratives
    course_eval["extras"] = extras

    return course_eval

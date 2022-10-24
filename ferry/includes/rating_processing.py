"""
Functions for processing ratings.

fetch_course_eval is used by /ferry/crawler/fetch_ratings.py.
"""
from typing import Any, Dict, List, Tuple

import requests
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


def fetch_questions(page, crn, term_code) -> Dict[QuestionId, str]:
    """
    Get list of question Ids for a certain course from OCE.

    Parameters
    ----------
    page:
        OCE page for the course
    crn:
        CRN of the course
    term_code:
        Term code that the course belongs to.
    Returns
    -------
    questions:
        Map from question IDs to question text.
    """

    soup = BeautifulSoup(page.content, "lxml")

    infos = soup.find("table", id="questions").find("tbody").find_all("tr")

    questions = {}

    for question_row in infos:
        question_id = question_row.find_all("td")[2].text.strip()
        question_text = question_row.find("td", class_="Question", recursive=False).find(text=True).text.strip()
        
        # print(question_id, question_text)
        questions[question_id] = question_text

    if len(questions) == 0:  # Evaluation data for this course not available
        raise CrawlerError(
            f"Evaluations for course crn={crn} in term={term_code} are unavailable"
        )

    return questions


def fetch_eval_ratings(
    page: requests.Response, question_id: str
) -> Tuple[List[int], List[str]]:
    """
    Fetch the evaluation ratings for the given question.

    Parameters
    ----------
    page:
        OCE page for the course
    question_id:
        Question ID
    Returns
    -------
    data:
        Response data for the question.
    options:
        Options for the question.
    """
    soup = BeautifulSoup(page.content, "lxml")

	# Get the 0-indexed question index
    q_index = (
        soup.find("td", text=str(question_id))
        .parent.get("id")
        .replace("questionRow", "")
    )

    table = soup.find("table", id="answers" + str(q_index))

    rows = table.find("tbody").find_all("tr")

    ratings = []
    options = []
    for row in rows:
        item = row.find_all("td")
        options.append(item[0].text.strip()) # e.g. "very low"
        ratings.append(int(item[1].text.strip())) # e.g. 8

    # print(options, ratings)

    return ratings, options


def fetch_eval_comments(
    page: requests.Response, questions: Dict[QuestionId, str], question_id: str
) -> Dict[str, Any]:
    """
    Fetch the comments for the given narrative question.

    Parameters
    ----------
    page:
        OCE page for the course
    questions:
        Map from question IDs to question text.
    question_id:
        Question ID
    Returns
    -------
    Question ID, question text, and the comments for the question.
    """
    soup = BeautifulSoup(page.content, "lxml")

    # Get the 0-indexed question index
    q_index = (
        soup.find("td", text=str(question_id))
        .parent.get("id")
        .replace("questionRow", "")
    )

    table = soup.find("table", id="answers" + str(q_index))
    rows = table.find("tbody").find_all("tr")
    comments = []
    for row in rows:
        comment = row.find_all("td")[1].text.strip()
        comments.append(comment)

    # print("Question ID: ", question_id)
    # print("Question text: ", questions[question_id])
    # print("Comments:", comments)

    return {
        "question_id": question_id,
        "question_text": questions[question_id],
        "comments": comments,
    }


def fetch_course_enrollment(page: requests.Response) -> Tuple[Dict[str, int], Dict[str, Any]]:
    """
    Get enrollment statistics for this course.

    Parameters
    ----------
    page:
        OCE page for the course
    Returns
    -------
    stats, extras:
        A dictionary with statistics, a dictionary with extra info
    """

    soup = BeautifulSoup(page.content, "lxml")

    stats = {}

    infos = (
        soup.find("div", id="courseHeader")
        .find_all("div", class_="row")[0]
		.find_all("div", recursive=False)[-1]
    )

    enrolled = infos.find_all("div", class_='row')[0].find_all("div")[-1].text.strip()
    responded = infos.find_all("div", class_='row')[1].find_all("div")[-1].text.strip()

    stats["enrolled"] = int(enrolled)
    stats["responded"] = int(responded)

    title = (
        soup.find("div", id="courseHeader")
        .find_all("div", class_="row")[0]
        .find_all("div", recursive=False)[1]
        .find_all("span")[1]
        .text.strip()
    )

    # print(stats, title)
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

    # OCE website for evaluations
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
        config.DATA_DIR / f"rating_cache/questions_index/{term_code}_{crn_code}.html",
        "w",
    ) as file:
        file.write(str(page_index.content))

    # Enrollment data.
    enrollment, extras = fetch_course_enrollment(page_index)

    # Fetch ratings questions.
    try:
        questions = fetch_questions(page_index, crn_code, term_code)
    except _EvaluationsNotViewableError as err:
        questions = {}
        extras["not_viewable"] = str(err)

    # List of all narrative data.
    narrative_qids = ["YC401", "YC403", "YC409"]

    # Numeric evaluations data.
    ratings = []
    for question_id, text in questions.items():
        # Only process rating data here
        if(question_id not in narrative_qids):
            data, options = fetch_eval_ratings(page_index, question_id)
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
    for qid in narrative_qids:
        narratives.append(fetch_eval_comments(page_index, questions, qid))

    course_eval: Dict[str, Any] = {}
    course_eval["crn_code"] = crn_code
    course_eval["season"] = term_code
    course_eval["enrollment"] = enrollment
    course_eval["ratings"] = ratings
    course_eval["narratives"] = narratives
    course_eval["extras"] = extras

    print(course_eval)

    return course_eval

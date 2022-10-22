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

    soup = BeautifulSoup(page.content, "lxml")

    infos = soup.find("table", id="questions").find("tbody").findAll("tr")

    questions = {}

    for question_row in infos:
        question_id = question_row.findAll("td")[2].text.strip()
        question_text = question_row.findAll("div", {"class": "Question"})[
            0
        ].text.strip()

        questions[question_id] = question_text

    if len(questions) == 0:  # Evaluation data for this course not available
        raise CrawlerError(
            f"Evaluations for course crn={crn} in term={term_code} are unavailable"
        )

    return questions


def fetch_eval_data(
    page: requests.Response, question_id: str
) -> Tuple[List[int], List[str]]:
    """
    does something
    """
    soup = BeautifulSoup(page.content, "lxml")

    qid = (
        soup.find("td", text=str(question_id))
        .parent.get("id")
        .replace("questionRow", "")
    )

    table = soup.find("table", id="answers" + str(qid))

    rows = table.findChildren("tr")

    ratings = []
    options = []
    for row in rows:
        item = row.findChildren("td")
        ratings.append(item[1])
        options.append(item[0])

    return ratings, options


def fetch_comments(page: requests.Response, qid: str) -> Dict[str, Any]:
    """
    does something
    """
    soup = BeautifulSoup(page.content, "lxml")
    anchor = soup.find("td", text=qid).parent
    idd = anchor.get("id").replace("questionRow", "")
    txt = anchor.find("td", {"class": "Question"}).text.strip()
    table = soup.find("table", id="answers" + str(idd))
    rows = table.findChildren("tr")
    comments = []
    for row in rows:
        item = row.findChildren("td")
        comments.append(item[1])

    return {
        "question_id": qid,
        "question_text": txt,
        "comments": comments,
    }


def fetch_course_enrollment(
    page: requests.Response,
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

    soup = BeautifulSoup(page.content, "lxml")

    stats = {}

    infos = (
        soup.find("div", id="courseHeader")
        .find("div")
        .findAll("div")[-1]
        .findAll("div")
    )

    enrolled = infos[0].findAll("div")[-1].text.strip()
    responded = infos[1].findAll("div")[-1].text.strip()

    stats["enrolled"] = int(enrolled)
    stats["responded"] = int(responded)

    title = (
        soup.find("div", id="courseHeader")
        .find("div")
        .findAll("div")[1]
        .findAll("span")[1]
        .text.strip()
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

    print("In fetch_course_eval: TERM: ", term_code,"CRN CODE: ", crn_code)

    # Main website with number of questions
    url_index = "https://oce.app.yale.edu/ocedashboard/studentViewer/courseSummary"

    class_info = {
        "crn": crn_code,
        "termCode": term_code,
    }

    page_index = session.get(url_index, params=class_info)

    # print(page_index.request.headers)
    print(page_index.content)

    # if page_index.status_code != 200:  # Evaluation data for this term not available
    #     raise CrawlerError(f"Evaluations for term {term_code} are unavailable")

    # # save raw HTML in case we ever need it
    # with open(
    #     config.DATA_DIR / f"rating_cache/questions_index/{term_code}_{crn_code}.html",
    #     "w",
    # ) as file:
    #     # print(str(page_index.content))
    #     file.write(str(page_index.content))

    # # Enrollment data.
    # enrollment, extras = fetch_course_enrollment(page_index)

    # # Fetch ratings questions.
    # try:
    #     questions = fetch_questions(page_index, crn_code, term_code)
    # except _EvaluationsNotViewableError as err:
    #     questions = {}
    #     extras["not_viewable"] = str(err)

    # # Numeric evaluations data.
    # ratings = []
    # for question_id, text in questions.items():
    #     data, options = fetch_eval_data(page_index, question_id)
    #     ratings.append(
    #         {
    #             "question_id": question_id,
    #             "question_text": text,
    #             "options": options,
    #             "data": data,
    #         }
    #     )

    # # Narrative evaluations data.
    # narratives = []
    # qids = ["YC409", "YC403", "YC401"]
    # for qid in qids:
    #     narratives.append(fetch_comments(page_index, qid))

    # course_eval: Dict[str, Any] = {}
    # course_eval["crn_code"] = crn_code
    # course_eval["season"] = term_code
    # course_eval["enrollment"] = enrollment
    # course_eval["ratings"] = ratings
    # course_eval["narratives"] = narratives
    # course_eval["extras"] = extras

    # return course_eval

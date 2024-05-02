"""
Functions for processing ratings.

fetch_course_eval is used by /ferry/crawler/fetch_ratings.py.
"""
from pathlib import Path
from typing import Any
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from httpx import AsyncClient, Response

from ferry.utils import request, save_cache_json

QuestionId = str


class page_index_class:
    def __init__(self, path: Path | Response):
        super().__init__()
        if isinstance(path, Response):
            self.content = path.content
            return
        with open(path, "rb") as file:
            self.content = file.read()


class EmptyEvaluationError(Exception):
    """
    Object for empty evaluations exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class EmptyNarrativeError(Exception):
    """
    Object for empty narrative exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class CrawlerError(Exception):
    """
    Object for crawler exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class AuthError(Exception):
    """
    Object for auth exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


def fetch_questions(
    page, crn, term_code
) -> tuple[dict[QuestionId, str], dict[QuestionId, bool]]:
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
    question_is_narrative:
        Map from question IDs to whether the question is a narrative question.
    """

    soup = BeautifulSoup(page.content, "lxml")

    questions = soup.find("table", id="questions")

    if questions is None:
        raise EmptyEvaluationError(
            f"Evaluations for course crn={crn} in term={term_code} are empty"
        )

    infos = questions.find("tbody").find_all("tr")

    questions = {}
    question_is_narrative = {}

    for question_row in infos:
        question_id = question_row.find_all("td")[2].text.strip()
        question_text = question_row.find(
            "td", class_="Question", recursive=False
        ).find(text=True)

        if question_text is None:
            # skip any empty questions (which is possible due to errors in OCE)
            continue

        # Check if question is narrative
        question_response = (
            question_row.find_all("td", class_="Responses")[0]
            .find_all("span", class_="show-for-print")[0]
            .find(text=True)
        )
        if "Narrative" in question_response:
            question_is_narrative[question_id] = True
        else:
            question_is_narrative[question_id] = False

        questions[question_id] = question_text.text.strip()
        # print(question_id, question_is_narrative[question_id], questions[question_id])

    if len(questions) == 0:  # Evaluation data for this course not available
        raise CrawlerError(
            f"Evaluations for course crn={crn} in term={term_code} are unavailable"
        )

    return questions, question_is_narrative


def fetch_eval_ratings(
    page: requests.Response, question_id: str
) -> tuple[list[int], list[str]]:
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
        options.append(item[0].text.strip())  # e.g. "very low"
        ratings.append(int(item[1].text.strip()))  # e.g. 8

    # print(options, ratings)

    return ratings, options


def fetch_eval_comments(
    page: requests.Response, questions: dict[QuestionId, str], question_id: str
) -> dict[str, Any]:
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

    if question_id == "SU124":
        # account for question 10 of summer courses
        response_table_id = "answers{i}"
    else:
        # Get the 0-indexed question index
        q_index = (
            soup.find("td", text=str(question_id))
            .parent.get("id")
            .replace("questionRow", "")
        )
        response_table_id = "answers" + str(q_index)

    table = soup.find("table", id=response_table_id)

    if table is None:
        raise EmptyNarrativeError()

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


def fetch_course_enrollment(
    page: requests.Response,
) -> tuple[dict[str, int | None], dict[str, Any]]:
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

    stats: dict[str, int | None] = {}

    infos = (
        soup.find("div", id="courseHeader")
        .find_all("div", class_="row")[0]
        .find_all("div", recursive=False)[-1]
    )

    enrolled = infos.find_all("div", class_="row")[0].find_all("div")[-1].text.strip()
    responded = infos.find_all("div", class_="row")[1].find_all("div")[-1].text.strip()

    stats["enrolled"] = int(enrolled)
    stats["responses"] = int(responded)
    stats["declined"] = None  # legacy: used to have "declined" stats
    stats["no response"] = None  # legacy: used to have "no response" stats

    title = (
        soup.find("div", id="courseHeader")
        .find_all("div", class_="row")[0]
        .find_all("div", recursive=False)[1]
        .find_all("span")[1]
        .text.strip()
    )

    # print(stats, title)
    return stats, {"title": title}


# Does not return anything, only responsible for writing course evals to json cache
def process_course_eval(page_index, crn_code, term_code, path: Path):
    if page_index is None:
        return
    # Enrollment data.
    try:
        enrollment, extras = fetch_course_enrollment(page_index)
    except:
        # Enrollment data is not available - most likely error page was returned.
        return

    # Fetch questions.
    try:
        questions, question_is_narrative = fetch_questions(
            page_index, crn_code, term_code
        )
    except EmptyEvaluationError as err:
        questions = {}
        extras["not_viewable"] = str(err)

    # Fetch question responses based on whether they are narrative or rating.
    ratings = []
    narratives = []
    for question_id, text in questions.items():
        if question_is_narrative[question_id]:
            # fetch narrative responses
            try:
                narratives.append(
                    fetch_eval_comments(page_index, questions, question_id)
                )
            except EmptyNarrativeError:
                pass
        else:
            # fetch rating responses
            data, options = fetch_eval_ratings(page_index, question_id)
            ratings.append(
                {
                    "question_id": question_id,
                    "question_text": text,
                    "options": options,
                    "data": data,
                }
            )

    course_eval: dict[str, Any] = {}
    course_eval["crn_code"] = crn_code
    course_eval["season"] = term_code
    course_eval["enrollment"] = enrollment
    course_eval["ratings"] = ratings
    course_eval["narratives"] = narratives
    course_eval["extras"] = extras

    # print(course_eval)

    # Save to cache.
    save_cache_json(path, course_eval)


async def fetch_course_eval(
    client: AsyncClient, crn_code: str, term_code: str, data_dir: Path
) -> page_index_class:
    """
    Gets evaluation data and comments for the specified course in specified term.

    Parameters
    ----------
    client:
        The current session client with login cookie.
    crn_code:
        CRN of this course.
    term_code:
        term code of this course.
    data_dir:
        Path to data directory.

    Returns
    -------
    course_eval:
        Dictionary with all evaluation data.
    """
    questions_index = data_dir / "rating_cache" / "questions_index"
    html_file = questions_index / f"{term_code}_{crn_code}.html"
    if html_file.is_file():
        return page_index_class(html_file)

    # OCE website for evaluations
    url_eval = "https://oce.app.yale.edu/ocedashboard/studentViewer/courseSummary?"
    url_eval = url_eval + urlencode(
        {
            "crn": crn_code,
            "termCode": term_code,
        }
    )
    payload = {
        "cookie": client.cas_cookie,
        "url": url_eval,
    }
    try:
        page_index = await request(
            method="POST",
            url=client.url,
            client=client,
            json=payload,
        )
    except Exception as err:
        raise CrawlerError(
            f"Error fetching evaluations for {term_code}-{crn_code}: {err}"
        )

    if "Central Authentication Service" in page_index.text:
        raise AuthError(f"Cookie auth failed for {term_code}-{crn_code}")

    if page_index.status_code == 500:
        raise CrawlerError(
            f"Evaluations for term {term_code}-{crn_code} are unavailable"
        )

    if page_index.status_code != 200:  # Evaluation data for this term not available
        raise CrawlerError(f"Error fetching evaluations for {term_code}-{crn_code}")

    # save raw HTML in case we ever need it
    questions_index.mkdir(parents=True, exist_ok=True)
    with open(html_file, "wb") as file:
        file.write(page_index.content)

    return page_index_class(page_index)

import re
from pathlib import Path
from typing import Any, TypedDict, cast

from bs4 import BeautifulSoup, ResultSet, Tag

from ferry.crawler.cache import save_cache_json


class EmptyEvaluationError(Exception):
    pass


class EmptyNarrativeError(Exception):
    pass


def parse_questions(
    page: bytes, crn: str, season_code: str
) -> tuple[dict[str, str], dict[str, bool]]:
    soup = BeautifulSoup(page, "lxml")

    questions = soup.find("table", id="questions")

    if questions is None:
        raise EmptyEvaluationError(
            f"Evaluations for course crn={crn} in term={season_code} are empty"
        )

    infos = questions.find("tbody")
    if type(infos) == Tag:
        infos = cast(ResultSet[Tag], infos.find_all("tr"))
    else:
        raise EmptyEvaluationError()

    questions = {}
    question_is_narrative = {}

    for question_row in infos:
        question_code = question_row.find_all("td")[2].text.strip()
        question_text = question_row.find("td", class_="Question", recursive=False)
        question_text = question_text.find(text=True) if question_text else None

        if question_text is None:
            # skip any empty questions (which is possible due to errors in OCE)
            continue
        question_text = question_text.text.strip()
        # Some old questions contain the course code in the question text
        question_text = re.sub(
            r"[A-Z]+ \d+(?: \d+)?(?:/[A-Z]+ \d+(?: \d+)?)*",
            "this course",
            question_text,
        )

        # Check if question is narrative
        question_response = (
            question_row.find_all("td", class_="Responses")[0]
            .find_all("span", class_="show-for-print")[0]
            .find(text=True)
        )
        question_is_narrative[question_code] = "Narrative" in question_response

        questions[question_code] = question_text

    if len(questions) == 0:  # Evaluation data for this course not available
        raise EmptyEvaluationError(
            f"Evaluations for course crn={crn} in term={season_code} are unavailable"
        )

    return questions, question_is_narrative


class ParsedEvalRatings(TypedDict):
    question_code: str
    question_text: str
    options: list[str]
    data: list[int]


def parse_eval_ratings(
    page: bytes, questions: dict[str, str], question_code: str
) -> ParsedEvalRatings:
    soup = BeautifulSoup(page, "lxml")

    td = soup.find("td", text=str(question_code))
    if td is None or td.parent is None:
        raise EmptyEvaluationError()
    id = td.parent.get("id")
    if type(id) != str:
        raise EmptyEvaluationError()

    # Get the 0-indexed question index
    q_index = id.replace("questionRow", "")

    table = soup.find("table", id="answers" + q_index)
    if table is None:
        raise EmptyEvaluationError()

    tbody = table.find("tbody")
    if type(tbody) == Tag:
        rows = cast(ResultSet[Tag], tbody.find_all("tr"))
    else:
        raise EmptyEvaluationError()

    ratings: list[int] = []
    options: list[str] = []
    for row in rows:
        item = row.find_all("td")
        options.append(item[0].text.strip())  # e.g. "very low"
        ratings.append(int(item[1].text.strip()))  # e.g. 8

    # print(options, ratings)

    return {
        "question_code": question_code,
        "question_text": questions[question_code],
        "options": options,
        "data": ratings,
    }


class ParsedEvalComments(TypedDict):
    question_code: str
    question_text: str
    comments: list[str]


def parse_eval_comments(
    page: bytes, questions: dict[str, str], question_code: str
) -> ParsedEvalComments:
    soup = BeautifulSoup(page, "lxml")

    if question_code == "SU124":
        # account for question 10 of summer courses
        response_table_id = "answers{i}"
    else:
        td = soup.find("td", text=str(question_code))
        if td is None or td.parent is None:
            raise EmptyEvaluationError()
        id = td.parent.get("id")
        if type(id) != str:
            raise EmptyEvaluationError()
        # Get the 0-indexed question index
        q_index = id.replace("questionRow", "")
        response_table_id = "answers" + q_index

    table = soup.find("table", id=response_table_id)

    if table is None:
        raise EmptyNarrativeError()

    rows = table.find("tbody")
    if type(rows) == Tag:
        rows = rows.find_all("tr")
    else:
        raise EmptyNarrativeError()
    comments: list[str] = []
    for row in rows:
        comment = row.find_all("td")[1].text.strip().replace("\r", "")
        comments.append(comment)

    return {
        "question_code": question_code,
        "question_text": questions[question_code],
        "comments": comments,
    }


def parse_course_header(page: bytes) -> tuple[tuple[int, int], dict[str, Any]]:
    soup = BeautifulSoup(page, "lxml")

    header = soup.find("div", id="courseHeader")
    if type(header) != Tag:
        raise EmptyEvaluationError()
    header = header.find("div", class_="row")
    if type(header) != Tag:
        raise EmptyEvaluationError()
    infos = header.find_all("div", recursive=False)[-1]

    enrolled = infos.find_all("div", class_="row")[0].find_all("div")[-1].text.strip()
    responded = infos.find_all("div", class_="row")[1].find_all("div")[-1].text.strip()

    title = header.find_all("div", recursive=False)[1]
    if type(title) != Tag:
        raise EmptyEvaluationError()
    title = title.find_all("span")[1].text.strip()

    return (int(enrolled), int(responded)), {"title": title}


class ParsedEval(TypedDict):
    crn: str
    season: str
    enrolled: int  # Note: historical evals have None
    responses: int  # Note: historical evals have None
    ratings: list[ParsedEvalRatings]
    narratives: list[ParsedEvalComments]
    # Note: known extra keys are:
    # - title: str
    # - not_viewable: str
    # For historical evals:
    # - subject: str
    # - number: str
    # - section: int
    # - note: str
    extras: dict[str, Any]


def parse_eval_page(
    page_index: bytes | None, crn: str, season_code: str
) -> ParsedEval | None:
    if page_index is None:
        return None
    try:
        (enrolled, responses), extras = parse_course_header(page_index)
    except:
        # Enrollment data is not available - most likely error page was returned.
        return None

    try:
        questions, question_is_narrative = parse_questions(page_index, crn, season_code)
    except EmptyEvaluationError as err:
        questions = {}
        question_is_narrative = {}
        extras["not_viewable"] = str(err)

    # Fetch question responses based on whether they are narrative or rating.
    ratings: list[ParsedEvalRatings] = []
    narratives: list[ParsedEvalComments] = []
    for question_code in questions.keys():
        if question_is_narrative[question_code]:
            try:
                narratives.append(
                    parse_eval_comments(page_index, questions, question_code)
                )
            except EmptyNarrativeError:
                pass
        else:
            ratings.append(parse_eval_ratings(page_index, questions, question_code))

    return {
        "crn": crn,
        "season": season_code,
        "enrolled": enrolled,
        "responses": responses,
        "ratings": ratings,
        "narratives": narratives,
        "extras": extras,
    }

from pathlib import Path
from typing import Any, TypedDict, cast
import re
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
        question_id = question_row.find_all("td")[2].text.strip()
        question_text = question_row.find("td", class_="Question", recursive=False)
        question_text = question_text.find(text=True) if question_text else None

        if question_text is None:
            # skip any empty questions (which is possible due to errors in OCE)
            continue
        question_text = question_text.text.strip()
        # Some old questions contain the course code in the question text
        question_text = re.sub(r"[A-Z]+ \d+(?: \d+)?(?:/[A-Z]+ \d+(?: \d+)?)*", "this course", question_text)

        # Check if question is narrative
        question_response = (
            question_row.find_all("td", class_="Responses")[0]
            .find_all("span", class_="show-for-print")[0]
            .find(text=True)
        )
        question_is_narrative[question_id] = "Narrative" in question_response

        questions[question_id] = question_text
        # print(question_id, question_is_narrative[question_id], questions[question_id])

    if len(questions) == 0:  # Evaluation data for this course not available
        raise EmptyEvaluationError(
            f"Evaluations for course crn={crn} in term={season_code} are unavailable"
        )

    return questions, question_is_narrative


class ParsedEvalRatings(TypedDict):
    question_id: str
    question_text: str
    options: list[str]
    data: list[int]


def parse_eval_ratings(
    page: bytes, questions: dict[str, str], question_id: str
) -> ParsedEvalRatings:
    soup = BeautifulSoup(page, "lxml")

    td = soup.find("td", text=str(question_id))
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
        "question_id": question_id,
        "question_text": questions[question_id],
        "options": options,
        "data": ratings,
    }


class ParsedEvalComments(TypedDict):
    question_id: str
    question_text: str
    comments: list[str]


def parse_eval_comments(
    page: bytes, questions: dict[str, str], question_id: str
) -> ParsedEvalComments:
    soup = BeautifulSoup(page, "lxml")

    if question_id == "SU124":
        # account for question 10 of summer courses
        response_table_id = "answers{i}"
    else:
        td = soup.find("td", text=str(question_id))
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
        "question_id": question_id,
        "question_text": questions[question_id],
        "comments": comments,
    }


ParsedStats = TypedDict(
    "ParsedStats",
    {
        "enrolled": int,  # Note: historical evals have None
        "responses": int,  # Note: historical evals have None
        "declined": int | None,
        "no response": int | None,
    },
)


def parse_course_enrollment(page: bytes) -> tuple[ParsedStats, dict[str, Any]]:
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

    stats: ParsedStats = {
        "enrolled": int(enrolled),
        "responses": int(responded),
        "declined": None,  # legacy: used to have "declined" stats
        "no response": None,  # legacy: used to have "no response" stats
    }

    title = header.find_all("div", recursive=False)[1]
    if type(title) != Tag:
        raise EmptyEvaluationError()
    title = title.find_all("span")[1].text.strip()

    # print(stats, title)
    return stats, {"title": title}


class ParsedEval(TypedDict):
    crn_code: str
    season: str
    enrollment: ParsedStats
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


# Does not return anything, only responsible for writing course evals to json cache
def parse_eval_page(
    page_index: bytes | None, crn_code: str, season_code: str, path: Path
):
    if page_index is None:
        return
    try:
        enrollment, extras = parse_course_enrollment(page_index)
    except:
        # Enrollment data is not available - most likely error page was returned.
        return

    try:
        questions, question_is_narrative = parse_questions(
            page_index, crn_code, season_code
        )
    except EmptyEvaluationError as err:
        questions = {}
        question_is_narrative = {}
        extras["not_viewable"] = str(err)

    # Fetch question responses based on whether they are narrative or rating.
    ratings: list[ParsedEvalRatings] = []
    narratives: list[ParsedEvalComments] = []
    for question_id in questions.keys():
        if question_is_narrative[question_id]:
            try:
                narratives.append(
                    parse_eval_comments(page_index, questions, question_id)
                )
            except EmptyNarrativeError:
                pass
        else:
            ratings.append(parse_eval_ratings(page_index, questions, question_id))

    course_eval: ParsedEval = {
        "crn_code": crn_code,
        "season": season_code,
        "enrollment": enrollment,
        "ratings": ratings,
        "narratives": narratives,
        "extras": extras,
    }

    save_cache_json(path, course_eval)

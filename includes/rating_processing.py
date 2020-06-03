from typing import Dict, List, Tuple
import requests
import json
import time
import lxml

from bs4 import BeautifulSoup

QuestionId = str

class SeasonMissingEvalsError(Exception):
    pass

class CourseMissingEvalsError(Exception):
    pass

def fetch_questions(session: requests.Session, crn: str, term_code: str) -> Dict[QuestionId, str]:
    """
    Get list of question Ids for a certain course from OCE

    Parameters
    ----------
    session: Requests session
        The current session with login cookie

    crn: string
        The crn code of the course

    term_code: string
        The term code that the course belongs to

    Returns
    -------
    questions: Mapping from question IDs to question text
    """
    # Main website with number of questions
    url_index = "https://oce.app.yale.edu/oce-viewer/studentSummary/index" 

    # JSON data with question IDs
    url_show = "https://oce.app.yale.edu/oce-viewer/studentSummary/show" 

    class_info = {
        "crn": crn,
        "term_code": term_code,
    }

    page_index = session.get(url_index, params=class_info)
    if page_index.status_code != 200: # Evaluation data for this term not available
        raise SeasonMissingEvalsError(f"Evaluations for term {term_code} are unavailable")

    page_show = session.get(url_show)
    if page_show.status_code != 200: # Evaluation data for this course not available
        raise CourseMissingEvalsError(f"Evaluations for course crn={crn} in term={term_code} are unavailable")
    
    data_show = page_show.json()
    questionList = data_show['questionList']

    questions = {}
    for question in questionList:
        questionId = question['questionId']
        text = question['text']
        # Strip out "<br/> \r\n<br/><i>(Your anonymous response to this question may be viewed by Yale College students, faculty, and advisers to aid in course selection and evaluating teaching.)</i>"
        text = text[0:text.find("<br/>")]

        questions[questionId] = text

    if len(questions) == 0: # Evaluation data for this course not available
        raise CourseMissingEvalsError(f"Evaluations for course crn={crn} in term={term_code} are unavailable")
    
    return questions

def fetch_eval_data(session: requests.Session, questionId: QuestionId) -> Tuple[List[int], List[str]]:
    """
    Get rating data for each question of a course

    Parameters
    ----------
    session: Requests session
        The current session with login cookie

    questionId: string
        The questionId to fetch evaluation data for

    Returns
    -------
    ratings, options: evaluation data for the question ID, and the options
    """
    # JSON data with rating data for each question ID
    url_graphData = "https://oce.app.yale.edu/oce-viewer/studentSummary/graphData" 

    millis = int(round(time.time() * 1000)) # Get current time in milliseconds
    question_info = {
        "questionId": questionId,
        "_": millis
    }

    page_graphData = session.get(url_graphData, params = question_info) # Fetch ratings data
    if page_graphData.status_code != 200:
        raise CourseMissingEvalsError(f"missing ratings for {questionId}")
    data_graphData = json.loads(page_graphData.text)

    ratings = []
    options = []
    for item in data_graphData[0]['data']:
        ratings.append(item[1])
        options.append(item[0])

    return ratings, options

def fetch_comments(session: requests.Session, offset: int, _max: int) -> Tuple[QuestionId, str, List[str]]:
    """
    Get comments for a specific question of this course

    Parameters
    ----------
    session: Requests session
        The current session with login cookie

    offset: integer
        The question to fetch comments for. 0-indexed

    _max: integer
        Always 1. Just passed into get function

    Returns
    -------
    id,question,responses_text: string that holds question and list of strings that hold responses respectively
    """
    # Website with student comments for this question
    url_comments = "https://oce.app.yale.edu/oce-viewer/studentComments/index"

    comment_info = {
        "offset": offset,
        "max": _max
    }

    page_comments = session.get(url_comments, params = comment_info)

    if page_comments.status_code != 200:
        # Question doesn't exist
        raise CourseMissingEvalsError('no more evals available')

    soup = BeautifulSoup(page_comments.content, 'lxml')

    question_html = soup.find(id = "cList")

    # Question text.
    question = question_html.select_one('div > p:nth-of-type(2)').contents[0].strip()
    if question == None or question == "":
        raise CourseMissingEvalsError('no more evals available')

    # Question ID.
    info_area = question_html.select_one('div > p:nth-of-type(3)')
    questionId = info_area.contents[1].strip()

    # Responses.
    responses_text = [] # List of responses
    responses = soup.find_all(id = "answer")
    for answer_area in responses:
        answer = answer_area.find(id="text").get_text(strip=True)
        responses_text.append(answer)

    return questionId, question, responses_text

def fetch_course_enrollment(session: requests.Session, crn: str, term_code: str) -> Dict[str, int]:
    """
    Get enrollment statistics for this course

    Parameters
    ----------
    session: Requests session
        The current session with login cookie

    crn: string
        The crn code of the course

    term_code: string
        The term code that the course belongs to

    Returns
    -------
    stats: a dictionary with statistics
    """
    # Main website with number of questions
    url_index = "https://oce.app.yale.edu/oce-viewer/studentSummary/index" 

    class_info = {
        "crn": crn,
        "term_code": term_code,
    }
    page_index = session.get(url_index, params=class_info)
    if page_index.status_code != 200: # Evaluation data for this term not available
        raise CourseMissingEvalsError("missing enrollment data")
    
    soup = BeautifulSoup(page_index.content, 'lxml')

    stats = {}
    stats_area = soup.find(id='status')
    for item in stats_area.find_all('li'):
        stat = item.p
        if not stat.get_text(strip=True):
            continue
        name = stat.contents[0].strip()[:-1].lower()
        value = int(stat.contents[1].get_text())
        stats[name] = value

    return stats

def fetch_course_eval(session, crn_code,term_code):

    """
    Gets evaluation data and comments for the specified course in specified term

    Parameters
    ----------
    session: Requests session
        The current session with login cookie

    crn_code: string
        crn code of this course

    term_code: string
        term code of this course

    Returns
    -------
    course_eval: Dictionary with all evaluation data and integer that specifies 
                               whether or not this term has eval data respectively
    """

    print("TERM:",term_code,"CRN CODE:",crn_code)

    # Enrollment data.
    enrollment = fetch_course_enrollment(session, crn_code, term_code)

    # Numeric evaluations data.
    ratings = []
    questions = fetch_questions(session, crn_code, term_code)
    for questionId, text in questions.items():
        data, options = fetch_eval_data(session, questionId)
        ratings.append({
            'question_id': questionId,
            'question_text': text,
            'options': options,
            'data': data,
        })

    # Narrative evaluations data.
    narratives = []
    offset = 0 # Start with first question
    while True:
        try:
            # Get questions with their respective responses
            questionId, question, comments = fetch_comments(session, offset, 1)

            narratives.append({
                'question_id': questionId,
                'question_text': question,
                'comments': comments,
            })
            offset += 1 # Increment to next question
        except CourseMissingEvalsError as e:
            if offset == 0:
                raise CourseMissingEvalsError('cannot fetch narrative comments') from e
            else:
                # No more questions are available -- normal situation.
                break

    course_eval = {}
    course_eval["crn_code"] = crn_code
    course_eval["season"] = term_code
    course_eval["enrollment"] = enrollment
    course_eval["ratings"] = ratings
    course_eval["narratives"] = narratives

    return course_eval

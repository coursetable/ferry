import requests
import json
import time
import lxml

from bs4 import BeautifulSoup

def fetch_questions(session, crn, term_code):
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
    questionText, questionIds: List of strings of question text and List of strings of question Ids respectively
    """
    # Main website with number of questions
    url_index = "https://oce.app.yale.edu/oce-viewer/studentSummary/index" 

    # JSON data with question IDs
    url_show = "https://oce.app.yale.edu/oce-viewer/studentSummary/show" 

    class_info = {
        "crn": crn,
        "term_code": term_code
    }

    page_index = session.get(url_index,params = class_info)
    if page_index.status_code != 200: # Evaluation data for this term not available
        print("Evaluation data for term:",term_code,"is unavailable")
        return 0,-2

    page_show = session.get(url_show)
    if page_show.status_code != 200: # Evaluation data for this course not available
        print("Evaluation data for course:",crn,"in term:",term_code,"is unavailable")
        return 0,-1
    
    data_show = json.loads(page_show.text)
    questionList = data_show['questionList']
    

    # List of question IDs
    questionIds = []
    # List of question text
    questionText = []
    for question in questionList:
        questionIds.append(question['questionId'])
        questionText.append(question['text'])
        questionText[-1] = questionText[-1][0:questionText[-1].find("<br/>")]

    numQuestions = len(questionIds)

    if numQuestions == 0: # Evaluation data for this course not available
        print("Evaluation data for course:",crn,"in term:",term_code,"is unavailable")
        return 0,-1
    
    return questionText, questionIds

def fetch_course_evals(session, questionIds):
    """
    Get rating data for each question of a course

    Parameters
    ----------
    session: Requests session
        The current session with login cookie

    questionIds: List of strings
        The list of questionIds to fetch evaluation
        data for

    Returns
    -------
    course_evals: 2D list with evaluation data for each question ID
    """
    # JSON data with rating data for each question ID
    url_graphData = "https://oce.app.yale.edu/oce-viewer/studentSummary/graphData" 

    # Holds evals of all questions for this course
    course_evals = []

    for Id in questionIds:
        millis = int(round(time.time() * 1000)) # Get current time in milliseconds
        question_info = {
            "questionId": Id,
            "_": millis
        }
        page_graphData = session.get(url_graphData, params = question_info) # Fetch ratings data

        if page_graphData.status_code != 200:
            return []

        data_graphData = json.loads(page_graphData.text)
        if (len(data_graphData) == 0):
            return []

        evals_data = [] # Holds evals for this question
        for x in range(len(data_graphData[0]['data'])): # Iterate through each 
            evals_data.append(data_graphData[0]['data'][x][1])
        course_evals.append(evals_data) # Append to list of evals

    return course_evals

def fetch_comments(session,offset, _max):

    # Website with student comments for this question
    url_comments = "https://oce.app.yale.edu/oce-viewer/studentComments/index"

    comment_info = {
        "offset": offset,
        "max": _max
    }

    page_comments = session.get(url_comments, params = comment_info)

    if page_comments.status_code != 200:
        #print("INVALID PAGE")
        return -1,-1

    soup = BeautifulSoup(page_comments.content, 'lxml')

    question_html = soup.find(id = "cList")
    ptags = question_html.find_all("p")
    question = None
    isQuestion = 0
    for tag in ptags:
        if tag.get_text() == "Q:":
            isQuestion = 1
        elif isQuestion == 1:
            question = tag.get_text()
            break
    if (question == None or question == ""):
        return -1,-1
    question = question[0:question.find("\n")]
    #print(question)
    responses = soup.find_all(id = "answer")
    responses_text = []

    for response in responses:
        responses_text.append(response.find(id = "text").get_text())
        #print(responses_text[-1])
        #print("\n\n")

    return question,responses_text
        
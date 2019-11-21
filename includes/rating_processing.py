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
        print("Evaluations for term:",term_code,"is unavailable")
        return 0,-2

    page_show = session.get(url_show)
    if page_show.status_code != 200: # Evaluation data for this course not available
        #print("Evaluations for course:",crn,"in term:",term_code,"is unavailable")
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
        #print("Evaluations for course:",crn,"in term:",term_code,"is unavailable")
        return 0,-1
    
    return questionText, questionIds

def fetch_eval_data(session, questionIds):
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
    question,responses_text: string that holds question and list of strings that hold responses respectively
    """
    # Website with student comments for this question
    url_comments = "https://oce.app.yale.edu/oce-viewer/studentComments/index"

    comment_info = {
        "offset": offset,
        "max": _max
    }

    page_comments = session.get(url_comments, params = comment_info)

    if page_comments.status_code != 200: # Question doesn't exist
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
            question = tag.get_text() # Find question in html
            break

    if (question == None or question == ""): # Question doesn't exist
        return -1,-1

    question = question[0:question.find("\n")]
    responses = soup.find_all(id = "answer")
    responses_text = [] # List of responses

    for response in responses:
        responses_text.append(response.find(id = "text").get_text()) # Append this response to list

    return question,responses_text # Return question and responses

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
    course_eval,term_has_eval: Dictionary with all evaluation data and integer that specifies 
                               whether or not this term has eval data respectively
    """

    # Initialize dictionary
    course_eval = {}
    course_eval["crn_code"] = crn_code
    course_eval["Evaluation_Questions"] = []
    course_eval["Evaluation_Data"] = []
    course_eval["Comments_Questions"] = []
    course_eval["Comments_List"] = []

    print("TERM:",term_code,"CRN CODE:",crn_code) # Print data to console for debugging

    questionText, questionIds = fetch_questions(session, crn_code,term_code) # Get questions
    if (questionIds == -2): # No evals for this term
        return course_eval,-1
    elif (questionIds == -1): # No evals for this course
        return course_eval,1

    course_eval["Evaluation_Questions"] = questionText
    course_eval["Evaluation_Data"] = fetch_eval_data(session, questionIds) # Get evaluation graph data

    offset = 0 # Start with first question
    comments_questions = []
    comments_list = []
    while (True):
        question,comments = fetch_comments(session,offset,1) # Get questions with their respective responses
        if question == -1: # No more questions
            break
        comments_questions.append(question)
        comments_list.append(comments)
        offset += 1 # Increment to next question

    course_eval["Comments_Questions"] = comments_questions
    course_eval["Comments_List"] = comments_list
    return course_eval,1 # Return all eval data in dictionary
        
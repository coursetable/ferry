import requests
import json
import time

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
    questionIds: List of strings of question Ids
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
        return -2

    page_show = session.get(url_show)
    if page_show.status_code != 200: # Evaluation data for this course not available
        print("Evaluation data for course:",crn,"in term:",term_code,"is unavailable")
        return -1
    
    data_show = json.loads(page_show.text)
    questionList = data_show['questionList']

    # List of question IDs
    questionIds = []
    for question in questionList:
        questionIds.append(question['questionId'])

    numQuestions = len(questionIds)

    if numQuestions == 0: # Evaluation data for this course not available
        print("Evaluation data for course:",crn,"in term:",term_code,"is unavailable")
        return -1
    
    return questionIds

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
        data_graphData = json.loads(page_graphData.text)

        evals_data = [] # Holds evals for this question
        for x in range(len(data_graphData[0]['data'])): # Iterate through each 
            evals_data.append(data_graphData[0]['data'][x][1])
        course_evals.append(evals_data) # Append to list of evals

    return course_evals

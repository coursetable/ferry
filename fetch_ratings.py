import requests
import getpass 
import json
import time
import re
from tqdm import tqdm
from bs4 import BeautifulSoup
from includes.class_processing import *

from os import listdir
from os.path import isfile, join

# Input NetID and password to login to Yale CAS
netid = input("Yale NetID: ")
password = getpass.getpass()
session = requests.Session()

# Login
r = session.post("https://secure.its.yale.edu/cas/login?username="+netid+"&password="+password)

print("Cookies: ", session.cookies.get_dict())

url_index = "https://oce.app.yale.edu/oce-viewer/studentSummary/index" # Main website with number of questions
url_show = "https://oce.app.yale.edu/oce-viewer/studentSummary/show" # JSON data with question IDs
url_graphData = "https://oce.app.yale.edu/oce-viewer/studentSummary/graphData" # JSON data with rating data for each question ID

# get the list of all course JSON files as previously fetched
term_jsons_path = "./api_output/term_courses/"
term_jsons = [f for f in listdir(
    term_jsons_path) if isfile(join(term_jsons_path, f))]
term_jsons = [x for x in term_jsons if x[-5:] == ".json"]

# load and parse term JSONs
for term_json_file in term_jsons:
    term_code = term_json_file[0:6] # Get term code from file name
    output_path = "./api_output/term_evals/{}.json".format(
            term_code)
    with open(term_jsons_path + term_json_file, "r") as f:
        term_json = json.load(f)
    
    noEvals = 0 # Whether or not this term has evaluation data

    term_evals = {}

    for term_course in term_json: # Loop through each course in the term
        crn_code = term_course['crn'] # Get crn code for this course

        term_evals[crn_code] = []
        print("TERM:",term_code,"CRN CODE:",crn_code)
        class_info = {
            "crn": crn_code,
            "term_code": term_code
        }

        page_index = session.get(url_index,params = class_info)
        if page_index.status_code != 200: # Evaluation data for this term not available
            print("Evaluation data for term:",term_code,"is unavailable")
            noEvals = 1
            break
        
        page_show = session.get(url_show)
        if page_show.status_code != 200: # Evaluation data for this course not available
            print("Evaluation data for course:",crn_code,"in term:",term_code,"is unavailable")
            continue
        data_show = json.loads(page_show.text)

        questionList = data_show['questionList']

        # List of question IDs
        questionIds = []
        for question in questionList:
            questionIds.append(question['questionId'])

        numQuestions = len(questionIds)

        if numQuestions == 0: # Evaluation data for this course not available
            print("Evaluation data for course:",crn_code,"in term:",term_code,"is unavailable")
            continue

        for Id in questionIds:
            millis = int(round(time.time() * 1000)) # Get current time in milliseconds
            question_info = {
                "questionId": Id,
                "_": millis
            }
            page_graphData = session.get(url_graphData, params = question_info) # Fetch ratings data
            data_graphData = json.loads(page_graphData.text)

            #print(data_graphData)
            evals_data = []
            for x in range(len(data_graphData[0]['data'])):
                evals_data.append(data_graphData[0]['data'][x][1])
            term_evals[crn_code].append(evals_data)

        print(term_evals[crn_code])
    if (noEvals == 1):
        continue
    output = json.dumps(term_evals, indent=4)
    with open(output_path, "w") as f:
        #f.write(json.dumps(term_evals, indent=4))
        f.write(re.sub(r'",\s+', '", ', output))

    """
    if noEvals == 1:
        print("NO EVALUATION DATA FOR TERM:", term_code)
    else:
        print("EVALUATION DATA FOR TERM:", term_code)
    """

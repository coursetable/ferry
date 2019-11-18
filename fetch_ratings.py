import requests
import getpass 
import json
import time
import re
import sys
from tqdm import tqdm
from includes.class_processing import fetch_terms
from includes.rating_processing import *

from os import listdir
from os.path import isfile, join

"""
================================================================
This script fetches course evaluation data from the Yale 
Online Course Evaluation (OCE), in JSON format.
================================================================
"""

# Input NetID and password to login to Yale CAS
netid = input("Yale NetID: ")
password = getpass.getpass()
session = requests.Session()

# Login
r = session.post("https://secure.its.yale.edu/cas/login?username="+netid+"&password="+password)

print("Cookies: ", session.cookies.get_dict())

# get the list of all course JSON files as previously fetched
term_jsons_path = "./api_output/term_courses/"

terms = fetch_terms()

testing = 0

# load and parse term JSONs
for term_code in terms:
    with open(term_jsons_path + term_code + ".json", "r") as f:
        term_json = json.load(f)
    
    noEvals = 0 # Whether or not this term has evaluation data

    for term_course in term_json: # Loop through each course in the term
        course_eval = {}
        crn_code = term_course['crn'] # Get crn code for this course
        course_eval["crn_code"] = crn_code

        print("TERM:",term_code,"CRN CODE:",crn_code)

        # Test code on CPSC 223 in term 201803
        if testing == 1:
            #crn_code = 10684 #CPSC 223
            term_code = 201803

        # term_code = 201803

        questionText, questionIds = fetch_questions(session, crn_code,term_code)
        if (questionIds == -2): # No evals for this term
            noEvals = 1
            break
        elif (questionIds == -1): # No evals for this course
            continue
        course_eval["Evaluation_Questions"] = questionText
        course_eval["Evaluation_Data"] = fetch_course_evals(session, questionIds)

        """
        for x in range(len(course_eval["Evaluation_Data"])):
            print("Question:",course_eval["Evaluation_Questions"][x])
            print("Evaluations:",course_eval["Evaluation_Data"][x],"\n")
        """

        offset = 0
        comments_questions = []
        comments_list = []
        while (True):
            question,comments = fetch_comments(session,offset,1)
            if question == -1:
                break
            comments_questions.append(question)
            comments_list.append(comments)
            offset += 1

        course_eval["Comments_Questions"] = comments_questions
        course_eval["Comments_List"] = comments_list

        """
        for x in range(len(course_eval["Comments_Questions"])):
            print("Question:",course_eval["Comments_Questions"][x],"\n")
            print(course_eval["Comments_List"][x], "\n\n")
        """
        course_unique_id = term_course["code"] + "-" + \
            term_course["crn"] + "-" + term_course["srcdb"]
        output_path = "./api_output/course_evals/{}.json".format(course_unique_id)
        with open(output_path, "w") as f:
            f.write(json.dumps(course_eval, indent=4))

        print("Evaluations for course:",course_unique_id,"dumped in JSON")

        if testing == 1:
            sys.exit()

    if (noEvals == 1): # No evaluations for this term
        continue

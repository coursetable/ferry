import requests
import getpass 
import json
import time
import re
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

# load and parse term JSONs
for term_code in terms:
    with open(term_jsons_path + term_code + ".json", "r") as f:
        term_json = json.load(f)
    
    noEvals = 0 # Whether or not this term has evaluation data

    term_evals = {} # Dictionary where each key holds the evals for a course

    for term_course in term_json: # Loop through each course in the term
        crn_code = term_course['crn'] # Get crn code for this course

        term_evals[crn_code] = [] # Initialize course eval with empty list

        print("TERM:",term_code,"CRN CODE:",crn_code)

        questionIds = fetch_questions(session, crn_code,term_code)
        if (questionIds == -2): # No evals for this term
            noEvals = 1
            break
        elif (questionIds == -1): # No evals for this course
            continue

        term_evals[crn_code] = fetch_course_evals(session, questionIds)

        print(term_evals[crn_code]) # Print evaluation data to console for debugging

    if (noEvals == 1): # No evaluations for this term
        continue

    output_path = "./api_output/term_evals/{}.json".format(term_code)
    output = json.dumps(term_evals, indent=4) # JSON output
    with open(output_path, "w") as f:
        f.write(output) # Cache JSON data to term_evals file


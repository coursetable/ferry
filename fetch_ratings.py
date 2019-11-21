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

# load and parse term JSONs
for term_code in terms:
    with open(term_jsons_path + term_code + ".json", "r") as f:
        term_json = json.load(f)

    for term_course in term_json: # Loop through each course in the term
        
        course_unique_id = term_course["code"] + "-" + \
            term_course["crn"] + "-" + term_course["srcdb"]
        output_path = "./api_output/course_evals/{}.json".format(course_unique_id)
        """
        if isfile(output_path):
            print("Evaluations for course:",course_unique_id,"already exists")
            continue
        """
        course_eval , term_has_eval = fetch_course_eval(session,term_course['crn'],term_code)

        if (term_has_eval == -1):
            break
        
        with open(output_path, "w") as f:
            f.write(json.dumps(course_eval, indent=4))

        print("Evaluations for course:",course_unique_id,"dumped in JSON")

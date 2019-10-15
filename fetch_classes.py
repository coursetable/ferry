from tqdm import tqdm
import json

from includes.class_processing import *

"""
================================================================
This script fetches the following information from the Yale 
Courses API, in JSON format:

    (1) A list of all terms with course info
        (/api_output/terms.json)

    (2) A list of all courses for each term
        (/api_output/term_courses/)

    (3) Detailed information for each course, for each term
        (/api_output/course_json_cache/)
================================================================

"""

# # list of all available terms
terms = fetch_terms()

with open("./api_output/terms.json", "w") as f:
    f.write(json.dumps(terms, indent=4))

# get lists of classes per term
for term in terms:
    print("Fetching class list for term {}".format(term))

    term_courses = fetch_term_courses(term)

    # cache list of classes
    with open("./api_output/term_courses/"+term+".json", "w") as f:
        f.write(json.dumps(term_courses, indent=4))

# fetch detailed info for each class in each term
for term in terms:

    with open("./api_output/term_courses/"+term+".json", "r") as f:
        term_courses = json.load(f)

    # track progress for each term
    pbar = tqdm(total=len(term_courses), ncols=96)
    pbar.set_description("Fetching class information for term {}".format(term))

    for course in term_courses:
        course_json = fetch_course_json(
            course["code"],
            course["crn"],
            course["srcdb"]
        )

        course_unique_id = course_json["code"] + "-" + \
            course_json["crn"] + "-" + course_json["srcdb"]

        # output JSON file
        output_path = "./api_output/course_json_cache/{}.json".format(
            course_unique_id)

        # cache to JSON
        with open(output_path, "w") as f:
            f.write(json.dumps(course_json, indent=4))

        pbar.update(1)

    print()

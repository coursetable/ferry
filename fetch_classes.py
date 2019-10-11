from tqdm import tqdm
import json

print("Test")

from includes.class_processing import *

terms = fetch_terms()

with open("./api_output/terms.json", "w") as f:
		f.write(json.dumps(terms, indent=4))

for term in terms:

	print("Fetching class list for term {}".format(term))

	term_courses = fetch_term_courses(term)

	with open("./api_output/term_courses/"+term+".json", "w") as f:
		f.write(json.dumps(term_courses, indent = 4))

for term in terms:

	print("Fetching class information for term {}".format(term))

	with open("./api_output/term_courses/"+term+".json", "r") as f:
		term_courses = json.load(f)

	pbar = tqdm(total = len(term_courses), ncols = 64)

	for course in term_courses:

	    course_json = fetch_course_json(
	        course["code"],
	        course["crn"],
	        course["srcdb"]
	    )

	    course_info = extract_course_info(course_json)

	    course_unique_id = course_json["code"] + "-" + course_json["crn"] + "-" + course_json["srcdb"]

	    with open("./api_output/course_infos/"+ course_unique_id + ".json", "w") as f:
	    	f.write(json.dumps(course_info, indent = 4))

	    pbar.update(1)


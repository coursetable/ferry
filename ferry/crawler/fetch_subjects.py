"""
Fetches a list of all seasons for the following:

    (1) courses (/api_output/course_subjects.json)
    (2) demand (/api_output/demand_subjects.json)

This list of seasons is then used and required by
fetch_classes.py, fetch_demand.py, and fetch_ratings.py.
"""

import requests
import ujson
from bs4 import BeautifulSoup

from ferry import config


class FetchSubjectsError(Exception):
    """
    Error object for fetch subjects exceptions.
    """

    pass


# ------------------------------------------
# Retrieve subjects from unofficial Yale API
# ------------------------------------------

print("Fetching subjects from courses.yale.edu... ", end="")
r = requests.post("https://courses.yale.edu/")

# Successful response
if r.status_code == 200:

    soup = BeautifulSoup(r.text, "html.parser")

    # get all the dropdown options and split into subject code + subject name
    subject_elems = soup.select("#crit-subject option")
    subject_names = [elem.text.split(" (", 2)[0] for elem in subject_elems[1:]]
    subject_codes = [elem.text.split(" (", 2)[1][:-1] for elem in subject_elems[1:]]
    course_subjects = dict(zip(subject_codes, subject_names))

    # write subjects list for use later
    with open(f"{config.DATA_DIR}/course_subjects.json", "w") as f:
        f.write(ujson.dumps(course_subjects, indent=4))

    print("ok")

# Unsuccessful
else:
    raise FetchSubjectsError(
        f"Unsuccessful course subjects response: code {r.status_code}"
    )

# -------------------------------------------
# Retrieve subjects from course demand portal
# -------------------------------------------

print("Fetching subjects from course demand portal... ", end="")
r = requests.get("https://ivy.yale.edu/course-stats/")

# Successful response
if r.status_code == 200:

    soup = BeautifulSoup(r.text, "html.parser")

    # get all the dropdown options and split into subject code + subject name
    subject_elems = soup.select("#subjectCode option")
    subject_codes = [elem.text.split(" - ", 2)[0] for elem in subject_elems[1:]]
    subject_names = [elem.text.split(" - ", 2)[1] for elem in subject_elems[1:]]
    demand_subjects = dict(zip(subject_codes, subject_names))

    # save the subjects for use in fetch_demand.py
    with open(f"{config.DATA_DIR}/demand_subjects.json", "w") as f:
        f.write(ujson.dumps(demand_subjects, indent=4))

    print("ok")

# Unsuccessful
else:
    raise FetchSubjectsError(
        f"Unsuccessful course subjects response: code {r.status_code}"
    )

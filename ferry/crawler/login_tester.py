import os
from pathlib import Path

import requests
import ujson
import csv

from ferry import config
from ferry.includes.cas import create_session
from ferry.includes.rating_processing import (
    fetch_course_enrollment,
    fetch_course_eval,
    fetch_questions,
)
from ferry.includes.class_processing import (
    fetch_course_json,
    fetch_season_courses
)
from ferry.includes.tqdm import tqdm

# cookie = "JSESSIONID=8D980F760BDE3AE0848C137F743F1AAB; _6a39a=http://10.0.0.234:8080; dtCookie=v_4_srv_4_sn_DDF1EE0C9AEE712BBDA4417F226ADB38_perc_100000_ol_0_mul_1_app-3A09c378fd1d50f13a_1_rcs-3Acss_0; rxVisitor=1666570329351H07B047OHB8PTJ06TIAV64NDHTMB5PCF; _ga=GA1.2.358349093.1666570356; _gid=GA1.2.1310142299.1666570356; dtSa=-; rxvt=1666624631534|1666622828084; dtPC=4$422830817_133h-vNVKPKOMLSMRHRUUUEVRKRPACHCRKDRCM-0e0; dtLatC=1"

session = create_session()

url_index = "https://oce.app.yale.edu/ocedashboard/studentViewer/courseSummary"

class_info = {
    "crn": "20356",
    "termCode": "202201",
}

page = session.get(url_index, params=class_info)
print(page.status_code)

print(page.text)

# season_courses = fetch_season_courses("202401", criteria=[])

# # cache list of classes
# with open("test.json", "w") as f:
#     ujson.dump(season_courses, f, indent=4)

# fetch_course_json("HUMS 021", "27525", "202401")

# # fetch_course_enrollment(page)

# # questions = fetch_questions(page, 0, 0)

# # fetch_eval_ratings(page, "YC402")

# # fetch_eval_comments(page, questions, "YC401")

# fetch_course_eval(session, "30379", "202102")
# print(fetch_course_eval(session, "12251", "202203"))

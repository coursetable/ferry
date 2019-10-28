from tqdm import tqdm
import json

from includes.class_processing import *

"""
================================================================
This script fetches existing course data on the current
CourseTable website and outputs the following into 
/api_output/previous_json/:

    (1) Course information, evaluations included

    (2) Course information, evaluations excluded
================================================================
"""

# get list of terms
with open("./api_output/terms.json", "r") as f:
    terms = json.load(f)

# get lists of classes per term
for term in terms:

    # evaluations included
    print("Fetching previous JSON for term {} (with evals)".format(term))
    previous_json = fetch_previous_json(term, evals=True)
    with open("./api_output/previous_json/evals_"+term+".json", "w") as f:
        f.write(json.dumps(previous_json, indent=4))

    # evaluations excluded
    print("Fetching previous JSON for term {} (without evals)".format(term))
    previous_json = fetch_previous_json(term, evals=False)
    with open("./api_output/previous_json/"+term+".json", "w") as f:
        f.write(json.dumps(previous_json, indent=4))

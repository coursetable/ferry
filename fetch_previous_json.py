from tqdm import tqdm
import json

from includes.class_processing import *


with open("./api_output/terms.json", "r") as f:
    terms = json.load(f)

# get lists of classes per term
for term in terms:
    print("Fetching previous JSON for term {} (with evals)".format(term))

    previous_json = fetch_previous_json(term, evals=True)

    # cache list of classes
    with open("./api_output/previous_json/"+term+".json", "w") as f:
        f.write(json.dumps(previous_json, indent=4))

    print("Fetching previous JSON for term {} (without evals)".format(term))

    previous_json = fetch_previous_json(term, evals=False)

    # cache list of classes
    with open("./api_output/previous_json/"+term+".json", "w") as f:
        f.write(json.dumps(previous_json, indent=4))

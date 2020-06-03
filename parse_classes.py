from tqdm import tqdm
import json

from includes.class_processing import *

from os import listdir
from os.path import isfile, join

import argparse

"""
================================================================
This script loads the class JSON files output by
fetch_classes.py and formats them to be loadable into the
current website.
================================================================
"""

# allow the user to specify seasons (useful for testing and debugging)
parser = argparse.ArgumentParser(description='Parse classes')
parser.add_argument('-s', '--seasons', nargs='+', help='seasons to parse', default=None, required=False)

args = parser.parse_args()
seasons = args.seasons

# folder to save course infos to
parsed_courses_folder = "./api_output/parsed_courses/"

if seasons is None:

    # get the list of all course JSON files as previously fetched
    with open("./api_output/seasons.json", "r") as f:
        seasons = json.load(f)

# load list of classes per term
for season in seasons:

    print("Parsing courses for term {}".format(term))

    # load raw responses for term
    with open("./api_output/course_json_cache/{}.json".format(term), "r") as f:
        aggregate_term_json = json.load(f)

    # parse course JSON in term
    parsed_course_info = [extract_course_info(x, season) for x in aggregate_term_json]

    # write output
    with open("./api_output/parsed_courses/{}.json".format(term), "w") as f:
        f.write(json.dumps(parsed_course_info, indent=4))
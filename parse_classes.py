from tqdm import tqdm
import json

from includes.class_processing import *

from os import listdir
from os.path import isfile, join


# get the list of all course JSON files as previously fetched
course_jsons_path = "./api_output/course_json_cache/"
course_jsons = [f for f in listdir(
    course_jsons_path) if isfile(join(course_jsons_path, f))]
course_jsons = [x for x in course_jsons if x[-5:] == ".json"]

# keep track of JSON loading progress
pbar = tqdm(total=len(course_jsons), ncols=96)

course_infos_path = "./api_output/course_infos/"

# load and parse course JSONs
for course_json_file in course_jsons:

    with open(course_jsons_path+course_json_file, "r") as f:
        course_json = json.load(f)

    # parse the relevant info
    course_info = extract_course_info(course_json)

    # output parsed JSON file path
    output_path = course_infos_path + course_json_file

    # output parsed course info
    with open(output_path, "w") as f:
        f.write(json.dumps(course_info, indent=4))

    pbar.update(1)

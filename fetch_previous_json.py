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

# define list of seasons

middle_years = [str(x) for x in range(2010, 2020)]

spring_seasons = [str(x) + "01" for x in middle_years]
summer_seasons = [str(x) + "02" for x in middle_years]
winter_seasons = [str(x) + "03" for x in middle_years]

seasons = [
    "200903",
    *spring_seasons,
    *summer_seasons,
    *winter_seasons,
    "202001", "202002"
]

# get lists of classes per season
for season in seasons:

    # evaluations included
    print(f"Fetching previous JSON for season {season} (with evals)")
    previous_json = fetch_previous_json(season, evals=True)
    with open(f"./api_output/previous_json/evals_{season}.json", "w") as f:
        f.write(json.dumps(previous_json, indent=4))

    # evaluations excluded
    print(f"Fetching previous JSON for season {season} (without evals)")
    previous_json = fetch_previous_json(season, evals=False)
    with open(f"./api_output/previous_json/{season}.json", "w") as f:
        f.write(json.dumps(previous_json, indent=4))

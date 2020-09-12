import ujson

from ferry import config
from ferry.includes.class_processing import *
from ferry.includes.tqdm import tqdm

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
seasons = fetch_previous_seasons()

# get lists of classes per season
for season in seasons:

    # evaluations included
    print(f"Fetching previous JSON for season {season} (with evals)")

    try:
        previous_json = fetch_previous_json(season, evals=True)

        with open(f"{config.DATA_DIR}/previous_json/evals_{season}.json", "w") as f:
            f.write(ujson.dumps(previous_json, indent=4))

    except FetchClassesError:
        print("JSON not found.")

    # evaluations excluded
    print(f"Fetching previous JSON for season {season} (without evals)")

    try:
        previous_json = fetch_previous_json(season, evals=False)
        with open(f"{config.DATA_DIR}/previous_json/{season}.json", "w") as f:
            f.write(ujson.dumps(previous_json, indent=4))

    except FetchClassesError:
        print("JSON not found.")

"""
This script fetches existing course data on the previous CourseTable website
and outputs the following into /api_output/previous_json/:

    (1) Course information, evaluations included

    (2) Course information, evaluations excluded

(Note that as of October 2020, this script no longer works as we have upgraded
the main site. However, the data files it produces are archived in our
ferry-data repository. If you are not part of the CourseTable team and are
interested in accessing these data, please contact us.)
"""

import ujson

from ferry import config
from ferry.includes.class_processing import (
    FetchClassesError,
    fetch_previous_json,
    fetch_previous_seasons,
)

# define list of seasons
seasons = fetch_previous_seasons()

# get lists of classes per season
for season in seasons:

    # evaluations included
    print(f"Fetching previous JSON for season {season} (with evals)")

    try:
        previous_json = fetch_previous_json(season, evals=True)

        with open(f"{config.DATA_DIR}/previous_json/evals_{season}.json", "w") as f:
            ujson.dump(previous_json, f, indent=4)

    except FetchClassesError:
        print("JSON not found.")

    # evaluations excluded
    print(f"Fetching previous JSON for season {season} (without evals)")

    try:
        previous_json = fetch_previous_json(season, evals=False)
        with open(f"{config.DATA_DIR}/previous_json/{season}.json", "w") as f:
            ujson.dump(previous_json, f, indent=4)

    except FetchClassesError:
        print("JSON not found.")

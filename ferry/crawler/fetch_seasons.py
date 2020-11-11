"""
Fetches a list of all seasons for the following:

    (1) courses (/api_output/course_seasons.json)
    (2) demand (/api_output/demand_seasons.json)

This list of seasons is then used and required by
fetch_classes.py, fetch_demand.py, and fetch_ratings.py.
"""

import re

import requests
import ujson
from bs4 import BeautifulSoup

from ferry import config


class FetchSeasonsError(Exception):
    """
    Error object for fetch seasons exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


# -----------------------------------------
# Retrieve seasons from unofficial Yale API
# -----------------------------------------

print("Fetching course seasons")
r = requests.post("https://courses.yale.edu/")

# Successful response
if r.status_code == 200:

    course_seasons = re.findall(r'option value="(\d{6})"', r.text)

    # exclude '999999' catch-all 'Past seasons' season option
    course_seasons = sorted([x for x in course_seasons if x != "999999"])

    # write seasons list for use later
    with open(f"{config.DATA_DIR}/course_seasons.json", "w") as f:
        f.write(ujson.dumps(course_seasons, indent=4))

# Unsuccessful
else:
    raise FetchSeasonsError(
        f"Unsuccessful course seasons response: code {r.status_code}"
    )


# ----------------------------------------------
# Retrieve seasons from course statistics portal
# ----------------------------------------------

print("Fetching demand seasons")
r = requests.get("https://ivy.yale.edu/course-stats/")

# Successful response
if r.status_code == 200:

    s = BeautifulSoup(r.text, "html.parser")

    season_elems = s.select("#termCode option[value]")
    demand_seasons = [elem.get("value") for elem in season_elems]

    # write seasons list for use later
    with open(f"{config.DATA_DIR}/demand_seasons.json", "w") as f:
        f.write(ujson.dumps(demand_seasons, indent=4))

# Unsuccessful
else:
    raise FetchSeasonsError(
        f"Unsuccessful demand seasons response: code {r.status_code}"
    )

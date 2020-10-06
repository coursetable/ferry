# Modified from Dan Zhao
# Original article: https://yaledailynews.com/blog/2020/01/10/yales-most-popular-courses/
# Github: https://github.com/iamdanzhao/yale-popular-classes
# README: https://github.com/iamdanzhao/yale-popular-classes/blob/master/data-guide/course_data_guide.md

# Import packages -----

import argparse
import os
import sys
from datetime import datetime
from multiprocessing import Pool

import requests
import ujson
from bs4 import BeautifulSoup

from ferry import config
from ferry.includes.demand_processing import get_dates, fetch_season_subject_demand


def handle_season_subject_demand(season, subject_code, subject_codes, dates):

    """
    Handler for fetching subject codes to be passed into
    Pool()
    """

    print(f"\t{subject_code}")

    courses = fetch_season_subject_demand(season, subject_code, subject_codes, dates)

    return courses


if __name__ == "__main__":

    class FetchDemandError(Exception):
        pass

    # Set season
    # Pass using command line arguments
    # Examples: 202001 = 2020 Spring, 201903 = 2019 Fall
    # If no season is provided, the program will scrape all available seasons
    parser = argparse.ArgumentParser(description="Import demand stats")
    parser.add_argument(
        "-s",
        "--seasons",
        nargs="+",
        help="seasons to fetch (leave empty to fetch all, or LATEST_[n] to fetch n latest)",
        default=None,
        required=False,
    )

    args = parser.parse_args()

    # list of seasons previously from fetch_seasons.py
    with open(f"{config.DATA_DIR}/demand_seasons.json", "r") as f:
        all_viable_seasons = ujson.loads(f.read())

    # if no seasons supplied, use all
    if args.seasons is None:

        seasons = all_viable_seasons

        print(f"Fetching ratings for all seasons: {seasons}")

    else:

        seasons_latest = len(args.seasons) == 1 and args.seasons[0].startswith("LATEST")

        # if fetching latest n seasons, truncate the list and log it
        if seasons_latest:

            num_latest = int(args.seasons[0].split("_")[1])

            seasons = all_viable_seasons[-num_latest:]

            print(f"Fetching ratings for latest {num_latest} seasons: {seasons}")

        # otherwise, use and check the user-supplied seasons
        else:

            # Check to make sure user-inputted seasons are valid
            if all(season in all_viable_seasons for season in args.seasons):

                seasons = args.seasons
                print(f"Fetching ratings for supplied seasons: {seasons}")

            else:
                raise FetchDemandError("Invalid season.")

    print("Retrieving subjects list... ", end="")
    with open(f"{config.DATA_DIR}/demand_subjects.json", "r") as f:
        subjects = ujson.loads(f.read())
        subject_codes = sorted(list(subjects.keys()))

    print("ok")

    for season in seasons:

        print(f"Retrieving demand for season {season}")

        dates = get_dates(season)

        # set up parallel processing pool
        pool = Pool(processes=64)

        pool_args = [
            [season, subject_code, subject_codes, dates]
            for subject_code in subject_codes
        ]

        season_courses = pool.starmap(handle_season_subject_demand, pool_args)

        print()

        # flatten season courses
        season_courses = [x for y in season_courses for x in y]

        # sort courses by title (for consistency with ferry-data)
        season_courses = sorted(season_courses, key=lambda x: x["title"])

        with open(f"{config.DATA_DIR}/demand_stats/{season}_demand.json", "w") as f:

            f.write(ujson.dumps(season_courses, indent=4))

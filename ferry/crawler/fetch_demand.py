"""
Fetches demand statistics.

Modified from Dan Zhao

Original article:
https://yaledailynews.com/blog/2020/01/10/yales-most-popular-courses/

Github:
https://github.com/iamdanzhao/yale-popular-classes

README:
https://github.com/iamdanzhao/yale-popular-classes/blob/master/data-guide/course_data_guide.md
"""

import argparse
from multiprocessing import Pool
from typing import List, Tuple

import ujson

from ferry import config
from ferry.crawler.common_args import add_seasons_args, parse_seasons_arg
from ferry.includes.demand_processing import fetch_season_subject_demand, get_dates
from ferry.includes.tqdm import tqdm

config.init_sentry()


def handle_season_subject_demand(demand_args: Tuple[str, str, List[str], List[str]]):

    """
    Handler for fetching subject codes to be passed into Pool()
    """

    demand_season, demand_subject_code, demand_subject_codes, demand_dates = demand_args

    courses = fetch_season_subject_demand(
        demand_season, demand_subject_code, demand_subject_codes, demand_dates
    )

    return courses


if __name__ == "__main__":

    class FetchDemandError(Exception):
        """
        Error object for demand fetching exceptions.
        """

        # pylint: disable=unnecessary-pass
        pass

    # Set season
    # Pass using command line arguments
    # Examples: 202001 = 2020 Spring, 201903 = 2019 Fall
    # If no season is provided, the program will scrape all available seasons
    parser = argparse.ArgumentParser(description="Import demand stats")
    add_seasons_args(parser)

    args = parser.parse_args()

    # list of seasons previously from fetch_seasons.py
    with open(f"{config.DATA_DIR}/demand_seasons.json", "r") as f:
        all_viable_seasons = ujson.load(f)

    seasons = parse_seasons_arg(args.seasons, all_viable_seasons)

    print("Retrieving subjects list... ", end="")
    with open(f"{config.DATA_DIR}/demand_subjects.json", "r") as f:
        subjects = ujson.load(f)
        subject_codes = sorted(list(subjects.keys()))

    print("ok")

    # set up parallel processing pool
    with Pool(processes=64) as pool:

        for season in seasons:

            print(f"Retrieving demand by subject for season {season}")

            dates = get_dates(season)

            pool_args = [
                (season, subject_code, subject_codes, dates)
                for subject_code in subject_codes
            ]

            season_courses = []

            # use imap_unordered to report to tqdm
            with tqdm(total=len(pool_args), desc="Subjects retrieved") as pbar:
                for i, result in enumerate(
                    pool.imap_unordered(handle_season_subject_demand, pool_args)
                ):
                    pbar.update()

                    season_courses.append(result)

            # flatten season courses
            season_courses = [x for y in season_courses for x in y]

            # sort courses by title (for consistency with ferry-data)
            season_courses = sorted(season_courses, key=lambda x: x["title"])

            with open(f"{config.DATA_DIR}/demand_stats/{season}_demand.json", "w") as f:
                ujson.dump(season_courses, f, indent=4)

# Modified from Dan Zhao
# Original article: https://yaledailynews.com/blog/2020/01/10/yales-most-popular-courses/
# Github: https://github.com/iamdanzhao/yale-popular-classes
# README: https://github.com/iamdanzhao/yale-popular-classes/blob/master/data-guide/course_data_guide.md

# Import packages -----

import argparse
import os
import sys
from datetime import datetime

import requests
import ujson
from bs4 import BeautifulSoup
from ferry import config
from ferry.includes.demand_processing import get_dates, get_subjects


class FetchDemandError(Exception):
    pass


# keep track of courses fetched over time
startTime = datetime.now()

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
            raise FetchClassesError("Invalid season.")

print("Retrieving subjects list... ", end="")
subjects = get_subjects()
print("ok")

for season in seasons:

    print("Retrieving dates with demand... ", end="")
    dates = get_dates(season)
    print("ok")

    # Scrape courses
    # Most of the code here is to deal with cross-listed courses, and to avoid having duplicate data

    courses = []  # containers for courses: format is title, codes, demand
    num_courses = 1

    for subject in subjects:
        # get URL and pass to BeautifulSoup
        # '.replace("&", "%26")' escapes the ampersand
        url = f'https://ivy.yale.edu/course-stats/?termCode={season}&subjectCode={subject.replace("&", "%26")}'
        r = requests.get(url)
        s = BeautifulSoup(r.text, "html.parser")

        # selects all the courses info and demand info
        # each element in course_containers contains code, name, and demand for one course
        course_containers = s.select("div#content > div > table > tbody > tr")

        for container in course_containers:
            course = []
            overall_demand = {}

            # extract name and code
            course_url = f'https://ivy.yale.edu{container.select("td a")[0]["href"]}'
            code = container.select("td a")[0].text.strip().replace(";", "")
            name = container.select("td span")[0].text.strip().replace(";", "")

            # 'code' might be a long list of cross-listed couses (e.g. 'S&DS 262/S&DS 562/CPSC 262'),
            # so we need to split all of the codes and look at them separately
            full_strings_all = code.split("/")

            # sometimes we'll get a course code that isn't actually an academic subject,
            # so this line filters that out
            full_strings = [
                string
                for string in full_strings_all
                if string.split(" ")[0] in subjects
            ]

            # now, we need to identify the course code corresponding to the subject we're working
            # on in the loop — this finds the course code with 'subject' in it
            code_this_subject = [
                string for string in full_strings if subject in string
            ][0]

            # Get section data, if applicable
            course_r = requests.get(course_url)
            course_s = BeautifulSoup(course_r.text, "html.parser")
            section_dict = {}

            # Check whether the page has a table with section data
            section_text = course_s.find("th", text="Section\xa0\xa0")
            if section_text:
                section_table = section_text.find_parent("table")
                section_table_rows = section_table.select("tbody tr")
                for row in section_table_rows:
                    cells = row.select("td")
                    section_name = cells[0].string.strip()
                    section_demand = cells[2].string.strip()
                    section_dict[section_name] = section_demand

            # Test if we've already added the demand for this course (due to cross-listing) into the
            # data structure. We don't want duplicate data, so if we already have the demand, we simply skip it
            if full_strings[0] == code_this_subject:
                # if this is our first time coming across this course, we need to add all of the
                # cross-listed course numbers into our 'courses' list

                # selects each of the individual counts
                # each element in count is one count corresponding to one day
                counts = container.select("td.trendCell")

                # add the count for each into our overall_demand list
                for j in range(len(dates)):
                    overall_demand[dates[j]] = counts[j].text.strip()

                course = {
                    "title": name,
                    "codes": full_strings,
                    "overall_demand": overall_demand,
                    "section_demand": section_dict,
                }

                courses.append(course)

            num_courses += 1

        print(
            f"Scraped {str(num_courses)} courses up to {subject}, {str(datetime.now() - startTime)} elapsed"
        )

    with open(f"{config.DATA_DIR}/demand_stats/{season}_demand.json", "w") as f:
        f.write(ujson.dumps(courses))

    print(f"Completed scraping {season}, {str(datetime.now() - startTime)} elapsed")

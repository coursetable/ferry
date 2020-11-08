"""
Functions for processing demand statistics.
Used by /ferry/crawler/fetch_demand.py
"""
import requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter

MAX_RETRIES = 16

SESSION = requests.Session()
SESSION.mount("http://", HTTPAdapter(max_retries=MAX_RETRIES))
SESSION.mount("https://", HTTPAdapter(max_retries=MAX_RETRIES))


class FetchDemandError(Exception):
    """
    Object for demand fetching exceptions.
    """

    pass


def get_dates(season):

    """
    Get dates with available course demand.

    Parameters
    ----------
    season: string
        The season to to get dates for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)

    Returns
    -------
    dates
    """

    # get URL and pass to BeautifulSoup
    # using AMTH as arbitary subject
    url = f"https://ivy.yale.edu/course-stats/?termCode={season}&subjectCode=AMTH"
    req = SESSION.get(url)

    if req.status_code != 200:

        raise FetchDemandError(f"Unsuccessful response: code {req.status_code}")

    dates_soup = BeautifulSoup(req.text, "html.parser")

    if dates_soup.title.text == "Error":
        print(f"Warning: no course demand dates found for season {season}")
        return []

    # select date elements
    dates_elems = dates_soup.select("table table")[0].select("td")

    dates = [date.text.strip() for date in dates_elems]

    return dates


def fetch_season_subject_demand(season, subject_code, subject_codes, dates):

    """
    Get course demand statistics for a specific subject and season

    Parameters
    ----------
    season: string
        The season to to get course demand for. In the form of
        YYYYSS(e.g. 201301 for spring, 201302 for summer,
        201303 for fall)

    subject_code: string
        Subject code to get course demand for.

    subject_codes: list of strings
        List of all subject codes (for validity checks)

    dates: list of strings
        List of all dates of interest (returned from get_dates)

    Returns
    -------
    demand: list
        list of {
            title
            codes
            overall_demand
            section_demand
        }
    """

    courses = []

    # get URL and pass to BeautifulSoup
    # '.replace("&", "%26")' escapes the ampersand
    demand_endpoint = "https://ivy.yale.edu/course-stats/"
    demand_args = f'?termCode={season}&subjectCode={subject_code.replace("&", "%26")}'
    url = f"{demand_endpoint}{demand_args}"
    req = SESSION.get(url)

    if req.status_code != 200:

        raise FetchDemandError(f"Unsuccessful response: code {req.status_code}")

    demand_soup = BeautifulSoup(req.text, "html.parser")

    # selects all the courses info and demand info
    # each element in course_containers contains code, name, and demand for one course
    course_containers = demand_soup.select("div#content > div > table > tbody > tr")

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
            if string.split(" ")[0] in subject_codes
        ]

        # now, we need to identify the course code corresponding to the subject we're working
        # on in the loop — this finds the course code with 'subject' in it
        code_this_subject = [
            string for string in full_strings if subject_code in string
        ][0]

        # Get section data, if applicable
        course_r = SESSION.get(course_url)
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
        # data structure. We don't want duplicate data, so if we already have the demand,
        # we simply skip it.

        if full_strings[0] == code_this_subject:
            # if this is our first time coming across this course, we need to add all of the
            # cross-listed course numbers into our 'courses' list

            # selects each of the individual counts
            # each element in count is one count corresponding to one day
            counts = container.select("td.trendCell")

            # add the count for each into our overall_demand list
            for date, count in zip(dates, counts):
                overall_demand[date] = count.text.strip()

            course = {
                "title": name,
                "codes": full_strings,
                "overall_demand": overall_demand,
                "section_demand": section_dict,
            }

            courses.append(course)

    return courses

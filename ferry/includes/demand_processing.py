import requests
import ujson
from bs4 import BeautifulSoup
from ferry import config


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
    r = requests.get(url)
    s = BeautifulSoup(r.text, "html.parser")

    # select date elements
    dates_elems = s.select("table table")[0].select("td")

    dates = [date.text.strip() for date in dates_elems]

    return dates

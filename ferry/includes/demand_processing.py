import requests
import ujson
from bs4 import BeautifulSoup
from ferry import config


def get_subjects():

    """
    Get list of all subjects.

    Returns
    -------
    subject_codes
    """

    url = "https://ivy.yale.edu/course-stats/"
    r = requests.get(url)
    s = BeautifulSoup(r.text, "html.parser")

    # get all the dropdown options and split into subject code + subject name
    subject_elems = s.select("#subjectCode option")
    subject_codes = [elem.text.split(" - ", 2)[0] for elem in subject_elems[1:]]
    subject_names = [elem.text.split(" - ", 2)[1] for elem in subject_elems[1:]]
    subject_dicts = [
        {"code": elem[0], "full_subject_name": elem[1]}
        for elem in zip(subject_codes, subject_names)
    ]

    # save the subjects in case we load it in another script
    with open(f"{config.DATA_DIR}/demand_stats/subjects.json", "w") as f:
        f.write(ujson.dumps(subject_dicts))

    return subject_codes


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

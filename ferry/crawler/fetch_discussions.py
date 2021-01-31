"""
Fetch discussion sections.

Pulls the discussion sections and locations PDF and parses to a CSV.
"""
import os

import regex as re
import requests
import tabula
from tika import parser

from ferry.config import DATA_DIR


class FetchDiscussionsError(Exception):
    """
    Error object for fetch discussion sections exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


def fetch_discussions():
    """
    Fetch discussion sections.

    """
    temp_output_path = DATA_DIR / "discussion_sections" / "raw_pdfs" / "temp.pdf"

    discussions = requests.get(
        "https://www.sis.yale.edu/buildings/Discussion_locations.pdf"
    )
    if discussions.status_code == 200:

        with open(temp_output_path, "wb") as file:
            file.write(discussions.content)
    # Unsuccessful
    else:
        raise FetchDiscussionsError(
            f"Unsuccessful discussion sections response: code {discussions.status_code}"
        )

    # get text dump of PDF
    raw = parser.from_file(str(temp_output_path))

    # match season from parsed text
    matches = re.search(
        r"Discussion section locations for ([A-Za-z0-9 ]+) Last generated on",
        raw["content"],
    )
    season = matches.group(1)

    # convert to season code
    year = season[-4:]
    season_raw = season.split(" ")[0]
    season_code = {"Spring": "01", "Summer": "02", "Fall": "03"}[season_raw]
    season = f"{year}{season_code}"

    discussions = tabula.read_pdf(
        temp_output_path,
        pandas_options={
            "names": ["section_crn", "subject", "number", "section", "info", "time"]
        },
        multiple_tables=False,
        stream=True,
        pages="all",
    )[0]

    discussions.to_csv(
        DATA_DIR / "discussion_sections" / "raw_csvs" / f"{season}.csv", index=False
    )

    # rename the PDF to its season
    os.rename(
        temp_output_path,
        DATA_DIR / "discussion_sections" / "raw_pdfs" / f"{season}.pdf",
    )


if __name__ == "__main__":
    fetch_discussions()  # pylint: disable=no-value-for-parameter

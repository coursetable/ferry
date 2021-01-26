"""
Fetch discussion sections.

Pulls the discussion sections and locations PDF and parses to a CSV.
"""
import click
import tabula

from ferry.config import DATA_DIR


@click.command()
@click.option(
    "-s",
    "--season",
    required=True,
    type=str,
    help="Season current discussion sections are under. Used for naming output file.",
)
def fetch_discussions(season: str):
    """
    Fetch discussion sections in a season.

    The season is not used to determine the source of the discussion sections, but rather
    is the season for which the current discussion sections posted at
    "https://www.sis.yale.edu/buildings/Discussion_locations.pdf" belong to.
    """
    discussions = tabula.read_pdf(
        "https://www.sis.yale.edu/buildings/Discussion_locations.pdf",
        pandas_options={
            "names": ["section_crn", "subject", "number", "section", "info", "time"]
        },
        multiple_tables=False,
        pages="all",
    )[0]

    discussions.to_csv(
        DATA_DIR / "discussion_sections" / "raw_csvs" / f"{season}.csv", index=False
    )


if __name__ == "__main__":
    fetch_discussions()  # pylint: disable=no-value-for-parameter

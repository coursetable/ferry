"""
Common command-line arguments
"""

from argparse import ArgumentParser
from typing import Any, List, Optional


class InvalidSeasonError(Exception):
    """
    Error object for invalid seasons exception.
    """

    # pylint: disable=unnecessary-pass
    pass


def add_seasons_args(parser: ArgumentParser):

    """
    Add ability to specify seasons.

    Parameters
    ----------
    parser: argparse.ArgumentParser
        ArgumentParser object
    """

    parser.add_argument(
        "-s",
        "--seasons",
        nargs="+",
        help="seasons to fetch (leave empty to fetch all, or LATEST_[n] to fetch n latest)",
        default=None,
        required=False,
    )


def parse_seasons_arg(arg_seasons: Optional[List[str]], all_viable_seasons: List[Any]):

    """
    Parse and handle seasons from add_seasons_args.

    Parameters
    ----------
    arg_seasons: list
        Seasons supplied as argument
    all_viable_seasons: list
        All allowed seasons
    """

    # if no seasons supplied, use all
    if arg_seasons is None:

        seasons = all_viable_seasons

        print(f"Fetching ratings for all seasons: {seasons}")

    else:

        seasons_latest = len(arg_seasons) == 1 and arg_seasons[0].startswith("LATEST")

        # if fetching latest n seasons, truncate the list and log it
        if seasons_latest:

            num_latest = int(arg_seasons[0].split("_")[1])

            seasons = all_viable_seasons[-num_latest:]

            print(f"Fetching ratings for latest {num_latest} seasons: {seasons}")

        # otherwise, use and check the user-supplied seasons
        else:

            # Check to make sure user-inputted seasons are valid
            if all(season in all_viable_seasons for season in arg_seasons):

                seasons = arg_seasons
                print(f"Fetching ratings for supplied seasons: {seasons}")

            else:
                raise InvalidSeasonError("Invalid season.")

    return seasons

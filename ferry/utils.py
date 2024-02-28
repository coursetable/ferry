"""
Utility functions for ferry driver.
"""

import argparse
import asyncio
import pathlib
from typing import Any

from httpx import AsyncClient


class RawArgs:
    cas_cookie: str | None
    config_file: str
    data_dir: str
    database_connect_string: str | None
    debug: bool | None
    fetch_classes: bool | None
    fetch_evals: bool | None
    parse_evals: bool | None
    generate_diagram: bool | None
    release: bool | None
    save_config: bool | None
    seasons: list[str] | None
    sentry_url: str | None
    sync_db: bool | None
    use_cache: bool | None

class Args:
    cas_cookie: str
    config_file: str
    client: AsyncClient
    data_dir: str
    database_connect_string: str
    debug: bool
    fetch_classes: bool
    fetch_evals: bool
    parse_evals: bool
    generate_diagram: bool
    release: bool
    seasons: list[str] | None
    sentry_url: str
    sync_db: bool
    use_cache: bool

class RateLimitError(Exception):
    """
    Error object for rate limit exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class InvalidSeasonError(Exception):
    """
    Error object for invalid seasons exception.
    """

    # pylint: disable=unnecessary-pass
    pass


_PROJECT_DIR = pathlib.Path(__file__).resolve().parent.parent

DATA_DIR = str(_PROJECT_DIR / "data")
CONFIG_FILE = str(_PROJECT_DIR / "config" / "config.yml")


# Args for main.py
def get_parser():
    parser = argparse.ArgumentParser(
        description="Ferry for Yale Course Selection",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "-r",
        "--release",
        help="Whether to run in release mode. This enables Sentry and requires a database connection string.",
        action="store_true",
    )

    parser.add_argument(
        "-d",
        "--debug",
        help="Whether to run in debug mode. Prints extra logs and information",
        action="store_true",
    )

    parser.add_argument(
        "--use-cache",
        help="Whether to use cache for requests. Automatically set to false in release mode.",
        action="store_true",
    )

    parser.add_argument(
        "--sync-db",
        help="Sync the database. This is automatically set to true in release mode.",
        action="store_true",
    )

    parser.add_argument(
        "--fetch-evals",
        help="Fetch evaluations.",
        action="store_true",
    )

    parser.add_argument(
        "--fetch-classes",
        help="Fetch classes.",
        action="store_true",
    )

    parser.add_argument(
        "--parse-evals",
        help="Parse evaluations.",
        action="store_true",
    )

    parser.add_argument(
        "--generate-diagram",
        help="Generate database diagram.",
        action="store_true",
    )

    parser.add_argument(
        "-f",
        "--config_file",
        help="Path to config YAML file. The config file will be loaded as default args.",
        default=CONFIG_FILE,
    )

    parser.add_argument(
        "--save-config",
        help="Save config YAML file. Overwrites existing file.",
        action="store_true",
    )

    parser.add_argument(
        "--data-dir",
        help="Directory to store data files.",
        default=DATA_DIR,
    )

    """
    CAS Authentication

    All cookies set in the request header are necessary for CAS authentication.

    TODO: Add more documentation on how to get these cookies.

    Example:
      JSESSIONID=...;
      dtCookie=...;
      _6a39a=...;
      _ga=...;
      _gid=...;
      _gat=...;
      _ga_NMGP3341FM=...
      [...]
    """
    parser.add_argument(
        "--cas-cookie",
        help="CAS cookie. If not specified, defaults to the value of the CAS_COOKIE environment variable before prompting user.",
        default=None,
    )

    parser.add_argument(
        "--database-connect-string",
        help="Database connection string. If not specified, defaults to the value of the MYSQL_URI environment variable before prompting user.",
        default=None,
    )

    parser.add_argument(
        "--sentry-url",
        help="Sentry URL. If not specified, defaults to the value of the SENTRY_URL environment variable before prompting user.",
        default=None,
    )

    parser.add_argument(
        "-s",
        "--seasons",
        nargs="+",
        help="Seasons to fetch. Format: Explicit list or LATEST_n to fetch n latest seasons. If not specified, fetches all seasons.",
        default=None,
    )

    return parser


def parse_seasons_arg(arg_seasons: list[str] | None, all_viable_seasons: list[Any]):
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

        print(f"All seasons: {seasons}")

    else:
        seasons_latest = len(arg_seasons) == 1 and arg_seasons[0].startswith("LATEST")

        # if fetching latest n seasons, truncate the list and log it
        if seasons_latest:
            num_latest = int(arg_seasons[0].split("_")[1])

            seasons = all_viable_seasons[-num_latest:]

            print(f"Latest {num_latest} seasons: {seasons}")

        # otherwise, use and check the user-supplied seasons
        else:
            # Check to make sure user-inputted seasons are valid
            if all(season in all_viable_seasons for season in arg_seasons):
                seasons = arg_seasons
                print(f"User-specified seasons: {seasons}")

            else:
                raise InvalidSeasonError("Invalid season.")

    return seasons


def parse_env_args(args: RawArgs):
    import os

    # Parse env var args
    if args.database_connect_string is None:
        args.database_connect_string = os.environ.get("POSTGRES_URI")
        if args.database_connect_string is None and args.sync_db:
            # prompt user
            args.database_connect_string = input("Enter database connection string: ")

    if args.sentry_url is None:
        args.sentry_url = os.environ.get("SENTRY_URL")
        if args.sentry_url is None and args.release:
            # prompt user
            args.sentry_url = input("Enter Sentry URL: ")

    if args.cas_cookie is None:
        args.cas_cookie = os.environ.get("CAS_COOKIE")
        if args.cas_cookie is None and args.fetch_evals:
            # prompt user
            args.cas_cookie = input("Enter CAS cookie: ")


def load_yaml(parser: argparse.ArgumentParser):
    p: RawArgs = parser.parse_args()

    if p.config_file is not None:
        try:
            # Check if config file exists
            from pathlib import Path

            if not Path(p.config_file).is_file():
                print(f"File not found: {p.config_file}.")
                return

            # Load YAML config as default args
            import yaml

            with open(p.config_file, "r") as f:
                default_arg: dict[str, Any] = yaml.safe_load(f)
            key = vars(p).keys()

            # Check for wrong args / empty file
            if default_arg is None:
                print(f"File is empty: {p.config_file}.")
                return
            for k in default_arg.keys():
                if k not in key:
                    print("WRONG ARG: {}".format(k))
                    assert k in key

            # Set default args
            parser.set_defaults(**default_arg)

        except:
            print(f"Error loading config file: {p.config_file}.")
            return


def save_yaml(args: RawArgs):
    if args.save_config:
        # Save config YAML file
        import yaml

        print(f"Saving config file to {args.config_file}.")

        del args.save_config
        config_file = str(args.config_file)
        data_dir = str(args.data_dir)

        del args.config_file

        if args.data_dir == DATA_DIR:
            del args.data_dir

        with open(config_file, "w+") as f:
            yaml.dump(vars(args), f)

        args.data_dir = data_dir


def get_args() -> Args:
    parser = get_parser()

    # Load YAML config as default args
    load_yaml(parser)

    args: RawArgs = parser.parse_args()

    # Set args.use_cache to False if args.release is True
    if args.release:
        args.use_cache = False

    # Save config YAML file if specified
    save_yaml(args)

    parse_env_args(args)

    # Initialize HTTPX client, only used for fetching classes (ratings fetch initializes its own client with CAS auth)
    args.client = AsyncClient(timeout=None)
    return args


# Init Sentry (in relase mode)
def init_sentry(sentry_url: str):
    import sentry_sdk

    if sentry_url is None:
        import os

        sentry_url = os.environ.get("SENTRY_URL")
        if sentry_url is None:
            raise SystemExit(
                "Error: SENTRY_URL is not set. It is required for production."
            )

    sentry_sdk.init(
        sentry_url,
        # Set traces_sample_rate to 1.0 to capture 100%
        # of transactions for performance monitoring.
        # We recommend adjusting this value in production.
        traces_sample_rate=1.0,
    )


def load_cache_json(path: str):
    """
    Load JSON from cache file

    Parameters
    ----------
    path: str
        Path to cache file

    Returns
    -------
    data: Any | None
        JSON data
    """
    import ujson

    p = pathlib.Path(path)

    if not p.is_file():
        return None

    with open(p, "r") as f:
        return ujson.load(f)


def save_cache_json(
    path: str,
    data: Any,
    indent: int = 4,
):
    """
    Save JSON to cache file

    Parameters
    ----------
    path: str
        Path to cache file
    data: Any
        Must be JSON serializable
    indent: int = 4
        Indentation for JSON file
    """
    import ujson

    p = pathlib.Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)

    with open(p, "w") as f:
        ujson.dump(data, f, indent=indent)


async def request(
    method: str,
    url: str,
    client: AsyncClient,
    attempts: int = 1,
    **kwargs,
):
    """
    Helper function to make a request with retries (exponential backoff)

    Parameters
    ----------
    method: str
        HTTP method
    url: str
        URL
    client: AsyncClient
        HTTPX AsyncClient
    attempts: int = 1
        Number of attempts
    **kwargs
        Additional keyword arguments for client.request
    """

    attempt = 0
    response = None

    while response is None and attempt < attempts:
        try:
            response = await client.request(method, url, **kwargs)
            if response.status_code == 429:
                raise RateLimitError()
        except:
            await asyncio.sleep(2**attempt)
            attempt += 1

    return response

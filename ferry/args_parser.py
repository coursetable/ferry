import argparse
from pathlib import Path
from typing import Any, cast


class RawArgs:
    cas_cookie: str | None
    config_file: str | None
    crawl_classes: bool
    crawl_evals: bool
    crawl_seasons: bool
    data_dir: str
    database_connect_string: str | None
    debug: bool
    generate_diagram: bool
    release: bool
    save_config: bool
    seasons: list[str] | None
    sentry_url: str | None
    snapshot_tables: bool
    sync_db: bool
    transform: bool
    use_cache: bool


class Args:
    cas_cookie: str
    crawl_classes: bool
    crawl_evals: bool
    crawl_seasons: bool
    data_dir: Path
    database_connect_string: str
    debug: bool
    generate_diagram: bool
    release: bool
    seasons: list[str] | None
    sentry_url: str
    snapshot_tables: bool
    sync_db: bool
    transform: bool
    use_cache: bool
    rewrite: bool


class InvalidSeasonError(Exception):
    pass


_PROJECT_DIR = Path(__file__).resolve().parent.parent

DATA_DIR = str(_PROJECT_DIR / "data")


def get_parser():
    parser = argparse.ArgumentParser(
        description="Ferry for Yale Course Selection",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--cas-cookie",
        help="CAS cookie. If not specified, defaults to the value of the CAS_COOKIE environment variable before prompting user.",
        default=None,
    )

    parser.add_argument(
        "-f",
        "--config-file",
        help="Path to config YAML file. The config file will be loaded as default args.",
        default=None,
    )

    parser.add_argument(
        "--crawl-classes",
        help="Crawl classes.",
        action="store_true",
    )

    parser.add_argument(
        "--crawl-evals",
        help="Crawl evaluations.",
        action="store_true",
    )

    parser.add_argument(
        "--crawl-seasons",
        help="Crawl seasons.",
        action="store_true",
    )

    parser.add_argument(
        "--data-dir",
        help="Directory to store data files.",
        default=DATA_DIR,
    )

    parser.add_argument(
        "--database-connect-string",
        help="Database connection string. If not specified, defaults to the value of the MYSQL_URI environment variable before prompting user.",
        default=None,
    )

    parser.add_argument(
        "-d",
        "--debug",
        help="Whether to run in debug mode. Prints extra logs and information",
        action="store_true",
    )

    parser.add_argument(
        "--generate-diagram",
        help="Generate database diagram.",
        action="store_true",
    )

    parser.add_argument(
        "-r",
        "--release",
        help="Whether to run in release mode. This enables Sentry and requires a database connection string.",
        action="store_true",
    )

    parser.add_argument(
        "--save-config",
        help="Save config YAML file. Overwrites existing file.",
        action="store_true",
    )

    parser.add_argument(
        "-s",
        "--seasons",
        nargs="+",
        help="Seasons to fetch. Format: Explicit list or LATEST_n to fetch n latest seasons. If not specified, fetches all seasons.",
        default=None,
    )

    parser.add_argument(
        "--sentry-url",
        help="Sentry URL. If not specified, defaults to the value of the SENTRY_URL environment variable before prompting user.",
        default=None,
    )

    parser.add_argument(
        "--snapshot-tables",
        help="Generate CSV files capturing data that would be written to DB.",
        action="store_true",
    )

    parser.add_argument(
        "--sync-db",
        help="Sync the database. This is automatically set to true in release mode.",
        action="store_true",
    )

    parser.add_argument(
        "--transform",
        help="Run the transformer",
        action="store_true",
    )

    parser.add_argument(
        "--use-cache",
        help="Whether to use cache for requests. Automatically set to false in release mode.",
        action="store_true",
    )

    parser.add_argument(
        "--rewrite",
        help="Whether to rewrite the database when syncing. Uses original sync_db function if true",
        action="store_true",
    )

    return parser


def parse_seasons_arg(
    arg_seasons: list[str] | None, all_viable_seasons: list[str] | None
):
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
        if all_viable_seasons is None:
            raise SystemExit(
                "No known viable seasons and no seasons provided in arguments"
            )
        seasons = all_viable_seasons

        print(f"All seasons: {seasons}")

    else:
        seasons_latest = len(arg_seasons) == 1 and arg_seasons[0].startswith("LATEST")

        # if fetching latest n seasons, truncate the list and log it
        if seasons_latest:
            if all_viable_seasons is None:
                raise SystemExit(f"No known viable seasons to resolve {seasons_latest}")
            num_latest = int(arg_seasons[0].split("_")[1])

            seasons = all_viable_seasons[-num_latest:]

            print(f"Latest {num_latest} seasons: {seasons}")

        # otherwise, use and check the user-supplied seasons
        else:
            if all_viable_seasons is None:
                # Trust args if there's nothing to validate with
                return arg_seasons
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
            args.database_connect_string = input("Enter database connection string: ")

    if args.sentry_url is None:
        args.sentry_url = os.environ.get("SENTRY_URL")
        if args.sentry_url is None and args.release:
            args.sentry_url = input("Enter Sentry URL: ")

    if args.cas_cookie is None:
        args.cas_cookie = os.environ.get("CAS_COOKIE")
        if args.cas_cookie is None and args.crawl_evals:
            args.cas_cookie = input("Enter CAS cookie: ")


def load_yaml(parser: argparse.ArgumentParser):
    p = cast(RawArgs, parser.parse_args())

    if p.config_file is None:
        return
    if not Path(p.config_file).is_file():
        print(f"File not found: {p.config_file}.")
        return
    try:
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
    # Save config YAML file
    import yaml

    if args.config_file is None:
        print("No config file specified.")
        return
    if not Path(args.config_file).is_file():
        print(f"File not found: {args.config_file}.")
        return

    print(f"Saving config file to {args.config_file}.")

    del args.save_config
    config_file = args.config_file
    data_dir = str(args.data_dir)

    del args.config_file

    if data_dir == DATA_DIR:
        del args.data_dir

    with open(config_file, "w+") as f:
        yaml.dump(vars(args), f)

    args.data_dir = data_dir


def get_args() -> Args:
    parser = get_parser()

    load_yaml(parser)

    args = cast(RawArgs, parser.parse_args())

    if args.release:
        args.use_cache = False

    if args.snapshot_tables or args.sync_db:
        args.transform = True

    if args.save_config:
        save_yaml(args)

    parse_env_args(args)

    final_args = cast(Args, args)

    final_args.data_dir = Path(final_args.data_dir)
    return final_args

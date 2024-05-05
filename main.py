import asyncio
from pathlib import Path

import uvloop

from ferry.crawler.classes import crawl_classes
from ferry.crawler.ratings import fetch_ratings, parse_ratings
from ferry.crawler.seasons import fetch_seasons
from ferry.transform import transform
from ferry.database import Database, stage, deploy
from ferry.args_parser import Args, get_args, parse_seasons_arg

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


# Init Sentry (in relase mode)
def init_sentry(sentry_url: str | None):
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


async def start_crawl(args: Args):
    classes = None
    # Fetch seasons
    course_seasons = await fetch_seasons(data_dir=args.data_dir, client=args.client)

    # Parse season args
    seasons = parse_seasons_arg(
        arg_seasons=args.seasons, all_viable_seasons=course_seasons
    )
    print("-" * 80)
    if args.fetch_classes:
        # Fetch classes/courses
        classes = await crawl_classes(
            seasons=seasons,
            data_dir=args.data_dir,
            client=args.client,
            use_cache=args.use_cache,
        )
    # Fetch ratings/evals
    if args.fetch_evals:
        await fetch_ratings(
            cas_cookie=args.cas_cookie,
            seasons=seasons,
            data_dir=args.data_dir,
            courses=classes,
        )
    elif args.parse_evals:
        # Make sure to parse evals since they are not cached in the data directory
        await parse_ratings(data_dir=args.data_dir)

    print("-" * 80)


def sync_db(args: Args):
    db = Database(args.database_connect_string)

    print("[Transform]")
    transform(data_dir=args.data_dir)
    print("-" * 80)

    print("[Stage]")
    stage(data_dir=args.data_dir, database=db)
    print("-" * 80)

    print("[Deploy]")
    deploy(db=db)
    print("-" * 80)
    print("Database sync: ✔")


async def main():
    args = get_args()

    if args.debug:
        import logging

        logging.basicConfig(level=logging.DEBUG)

    args.data_dir.mkdir(parents=True, exist_ok=True)

    if args.release:
        init_sentry(args.sentry_url)
    else:
        print("Running in dev mode. Sentry not initialized.")

    await start_crawl(args)
    await args.client.aclose()
    if args.sync_db:
        sync_db(args)
    if args.generate_diagram:
        from ferry.generate_db_diagram import generate_db_diagram

        generate_db_diagram(path=Path("docs/db_diagram.pdf"))


if __name__ == "__main__":
    asyncio.run(main())

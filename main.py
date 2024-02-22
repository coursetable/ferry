import asyncio
from pathlib import Path

import uvloop

from ferry.crawler.fetch_classes import fetch_classes
from ferry.crawler.fetch_ratings import fetch_ratings
from ferry.crawler.fetch_seasons import fetch_course_seasons
from ferry.database.database import Database
from ferry.deploy import deploy
from ferry.stage import stage
from ferry.transform import transform
from ferry.utils import (
    get_args,
    init_sentry,
    parse_seasons_arg,
    Args,
)

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def start_crawl(args: Args):
    # Fetch seasons
    course_seasons = await fetch_course_seasons(
        data_dir=args.data_dir, client=args.client
    )

    # Parse season args
    seasons = parse_seasons_arg(
        arg_seasons=args.seasons, all_viable_seasons=course_seasons
    )

    print("-" * 80)

    # Fetch classes/courses
    classes = await fetch_classes(
        seasons=seasons,
        data_dir=args.data_dir,
        client=args.client,
    )

    # Fetch ratings/evals
    if args.fetch_evals:
        await fetch_ratings(
            cas_cookie=args.cas_cookie,
            seasons=seasons,
            data_dir=args.data_dir,
            courses=classes,
        )

    print("-" * 80)

def sync_db(args: Args):
    db = Database(args.database_connect_string)

    stage(data_dir=Path(args.data_dir), database=db)
    print("-" * 80)

    transform(data_dir=Path(args.data_dir))
    print("-" * 80)

    deploy(db=db)
    print("-" * 80)
    print("Database sync: ✔")

async def main():
    args = get_args()

    # Create data directory if it doesn't exist
    Path(args.data_dir).mkdir(parents=True, exist_ok=True)

    # Initialize Sentry
    if args.release:
        init_sentry(args.sentry_url)
    else:
        print("Running in dev mode. Sentry not initialized.")

    # Start the crawl - fetch classes + ratings
    await start_crawl(args)

    # Close HTTPX client
    await args.client.aclose()

    # Sync the database
    if args.sync_db:
        sync_db(args)

    # Generate DB diagram
    if args.generate_diagram:
        from ferry.generate_db_diagram import generate_db_diagram

        generate_db_diagram(path="docs/db_diagram.pdf")


if __name__ == "__main__":
    asyncio.run(main())

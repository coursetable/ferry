import asyncio
import logging
from pathlib import Path
import pandas as pd

import uvloop
from httpx import AsyncClient

from ferry.args_parser import Args, get_args, parse_seasons_arg
from ferry.crawler.cache import load_cache_json
from ferry.crawler.classes import crawl_classes
from ferry.crawler.evals import crawl_evals
from ferry.crawler.seasons import fetch_seasons
from ferry.database import sync_db, sync_db_old
from ferry.transform import transform, write_csvs

asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.width", None)


async def start_crawl(args: Args):
    classes = None
    # Initialize HTTPX client, only used for fetching classes (evals fetch
    # initializes its own client with CAS auth)
    client = AsyncClient(timeout=None)
    if args.crawl_seasons:
        course_seasons = await fetch_seasons(
            data_dir=args.data_dir, client=client, use_cache=args.use_cache
        )
    else:
        # Still try to load from cache if it exists
        course_seasons = load_cache_json(args.data_dir / "course_seasons.json")
    seasons = parse_seasons_arg(
        arg_seasons=args.seasons, all_viable_seasons=course_seasons
    )
    print("-" * 80)
    if args.crawl_classes:
        classes = await crawl_classes(
            seasons=seasons,
            data_dir=args.data_dir,
            client=client,
            use_cache=args.use_cache,
        )
    if args.crawl_evals:
        await crawl_evals(
            cas_cookie=args.cas_cookie,
            seasons=seasons,
            data_dir=args.data_dir,
            courses=classes,
        )

    await client.aclose()
    print("-" * 80)


async def main():
    args = get_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)

    args.data_dir.mkdir(parents=True, exist_ok=True)

    if args.release:
        import sentry_sdk

        sentry_sdk.init(
            args.sentry_url,
            # Set traces_sample_rate to 1.0 to capture 100%
            # of transactions for performance monitoring.
            # We recommend adjusting this value in production.
            traces_sample_rate=1.0,
        )
    else:
        print("Running in dev mode. Sentry not initialized.")

    await start_crawl(args)
    tables = None
    if args.transform:
        tables = await transform(data_dir=args.data_dir)
    if args.snapshot_tables:
        assert tables
        write_csvs(tables, data_dir=args.data_dir)
    if args.sync_db:
        assert tables
        if args.rewrite:
            sync_db_old(tables, args.database_connect_string)
        else:
            sync_db(tables, args.database_connect_string, data_dir=args.data_dir)
    if args.generate_diagram:
        from ferry.generate_db_diagram import generate_db_diagram

        generate_db_diagram(path=Path("docs/db_diagram.pdf"))


if __name__ == "__main__":
    asyncio.run(main())

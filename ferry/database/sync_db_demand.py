import pandas as pd
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.orm import Session

from ferry.database import (
    Base,
    CourseDemandStatistics,
    Database,
    ListingDemandStatistics,
    session_scope,
)

BATCH_SIZE = 5000

STAT_COLUMNS = ["registered", "waitlisted", "visiting"]


def _upsert_batched(
    session: Session,
    table,
    records: list[dict],
    conflict_columns: list[str],
):
    for start in range(0, len(records), BATCH_SIZE):
        batch = records[start : start + BATCH_SIZE]
        stmt = pg_insert(table).values(batch)
        stmt = stmt.on_conflict_do_update(
            index_elements=conflict_columns,
            set_={col: getattr(stmt.excluded, col) for col in STAT_COLUMNS},
        )
        session.execute(stmt)


def sync_db_demand(tables: dict[str, pd.DataFrame], database_connect_string: str):
    """
    Upserts demand statistics into the database, at both the listing level
    (listing_demand_statistics) and the course/cross-listing-group level
    (course_demand_statistics, a sum across every listing in the group).

    Unlike courses (diff-based) or evals (drop-and-reload), demand data is
    treated as an append/upsert-only time series: once a date is in the
    past, its row is immutable, so rows are just upserted on their natural
    key, never diffed or dropped. listing_demand_statistics.id is left for
    postgres to auto-generate rather than reusing pandas' per-run sequential
    index, since this table accumulates across many separate runs instead of
    being recreated each time - reusing transform-generated ids would
    collide with ids already committed from earlier runs.
    course_demand_statistics has no surrogate id at all - its primary key is
    the natural (course_id, date) pair, since it's fully derived/recomputable
    from listing_demand_statistics and never targeted by other FKs.
    """
    listing_demand = tables["listing_demand_statistics"]
    course_demand = tables["course_demand_statistics"]

    db = Database(database_connect_string)
    Base.metadata.create_all(
        db.Engine,
        tables=[ListingDemandStatistics.__table__, CourseDemandStatistics.__table__],
    )

    if len(listing_demand) == 0 and len(course_demand) == 0:
        print("No demand statistics to sync.")
        return

    print(
        f"\nUpserting {len(listing_demand)} listing-level and "
        f"{len(course_demand)} course-level demand statistics..."
    )
    with session_scope(db.Session) as session:
        if len(listing_demand) > 0:
            _upsert_batched(
                session,
                ListingDemandStatistics.__table__,
                listing_demand.to_dict(orient="records"),
                conflict_columns=["listing_id", "date"],
            )
        if len(course_demand) > 0:
            _upsert_batched(
                session,
                CourseDemandStatistics.__table__,
                course_demand.to_dict(orient="records"),
                conflict_columns=["course_id", "date"],
            )

    print("\033[F", end="")
    print(
        f"Upserting {len(listing_demand)} listing-level and "
        f"{len(course_demand)} course-level demand statistics... ✔"
    )

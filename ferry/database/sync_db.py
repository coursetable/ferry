import pandas as pd
import csv
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect

from ferry import database
from ferry.database import Database, Base


queries_dir = Path(__file__).parent / "queries"


def sync_db(tables: dict[str, pd.DataFrame], database_connect_string: str):
    db = Database(database_connect_string)

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # First step: existing -> old
    print("\nMoving existing tables...")

    conn = db.Engine.connect()
    inspector = inspect(db.Engine)
    replace = conn.begin()
    conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
    for table in Base.metadata.sorted_tables:
        logging.debug(f"Updating table {table}")
        # If table doesn't exist, skip
        if table.name not in db_meta.tables:
            logging.debug(f"Table {table} does not exist in database.")
            continue
        # remove the old table if it is present before
        conn.execute(text(f"DROP TABLE IF EXISTS {table}_old CASCADE;"))
        for index in inspector.get_indexes(table.name):
            index_name = index["name"]
            if not index_name:
                continue
            conn.execute(text(f"ALTER INDEX {index_name} RENAME TO {index_name}_old"))

        for constraint in [
            inspector.get_pk_constraint(table.name),
            *inspector.get_foreign_keys(table.name),
            *inspector.get_unique_constraints(table.name),
        ]:
            name = constraint["name"]
            if not name:
                continue
            conn.execute(
                text(f"ALTER TABLE {table} RENAME CONSTRAINT {name} TO {name}_old")
            )
        # rename current main table to _old
        # (keep the old tables instead of dropping them
        # so we can rollback in case of errors)
        # Note that this is done after we've retrieved the indexes and constraints
        conn.execute(text(f'ALTER TABLE IF EXISTS "{table}" RENAME TO {table}_old;'))

    replace.commit()

    print("\033[F", end="")
    print("Moving existing tables... ✔")

    # Second step: stage new tables
    print("\nAdding new staging tables...")
    # TODO this should probably be done within one transaction
    conn = db.Engine.raw_connection()
    Base.metadata.create_all(db.Engine)
    for table in Base.metadata.sorted_tables:
        if table.name not in tables:
            raise ValueError(
                f"{table.name} defined in Base metadata, but there is no data for it."
            )
        # create in-memory buffer for DataFrame
        buffer = StringIO()
        # TODO is this really needed?
        tables[table.name] = tables[table.name].replace({r"\r": ""}, regex=True)
        tables[table.name].to_csv(
            buffer,
            index_label="id",
            header=False,
            index=False,
            sep="\t",
            quoting=csv.QUOTE_NONE,
            escapechar="\\",
            na_rep="NULL",
        )

        buffer.seek(0)
        cursor = conn.cursor()

        try:
            cursor.copy_from(
                buffer,
                table.name,
                columns=tables[table.name].columns,
                sep="\t",
                null="NULL",
            )
        except Exception as error:
            conn.rollback()
            cursor.close()
            raise error

        # print(f"Successfully copied {table_name}")
        cursor.close()
    conn.commit()

    print("\033[F", end="")
    print("Adding new staging tables... ✔")

    # Last: drop the _old tables
    print("\nDeleting temporary old tables...")

    conn = db.Engine.connect()
    delete = conn.begin()
    conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
    for table in Base.metadata.sorted_tables:
        logging.debug(f"Dropping table {table}_old")
        conn.execute(text(f"DROP TABLE IF EXISTS {table}_old CASCADE;"))
    delete.commit()

    print("\033[F", end="")
    print("Deleting temporary old tables... ✔")

    print("\nReindexing...")
    conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(text("REINDEX DATABASE postgres;"))
    print("\033[F", end="")
    print("Reindexing... ✔")

    # Print row counts for each table.
    print("\n[Table Statistics]")
    with database.session_scope(db.Session) as db_session:
        with open(queries_dir / "table_sizes.sql") as file:
            SUMMARY_SQL = file.read()

        result = db_session.execute(text(SUMMARY_SQL))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

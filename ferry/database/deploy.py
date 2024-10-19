import logging
from pathlib import Path

import sqlalchemy
from sqlalchemy import MetaData, text

from ferry import database

queries_dir = Path(__file__).parent / "queries"


def deploy(db: database.Database):
    """
    Deploy staged tables to main ones and regenerate database.
    """

    # --------------------------------------
    # Check if all staged tables are present
    # --------------------------------------

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # ordered tables defined only in our model
    alchemy_tables = database.Base.metadata.sorted_tables
    target_tables = [table.name[:-7] for table in alchemy_tables]

    db_tables = {x.name for x in db_meta.sorted_tables}

    if any(table.name not in db_tables for table in alchemy_tables):
        raise database.MissingTablesError(
            "Not all staged tables are present. Run stage.py again?"
        )

    # -------------------------------------
    # Upgrade staged tables to primary ones
    # -------------------------------------

    print("\nReplacing old tables with staged...")

    conn = db.Engine.connect()

    # keep track of main table constraints and indexes
    # because staged tables do not have foreign key relationships

    replace = conn.begin()
    conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))

    # drop and update tables in reverse dependency order
    for table in target_tables:

        logging.debug(f"Updating table {table}")

        # remove the old table if it is present before
        conn.execute(text(f"DROP TABLE IF EXISTS {table}_old CASCADE;"))
        # rename current main table to _old
        # (keep the old tables instead of dropping them
        # so we can rollback if invariants don't pass)
        conn.execute(text(f'ALTER TABLE IF EXISTS "{table}" RENAME TO {table}_old;'))
        # rename staged table to main
        conn.execute(text(f'ALTER TABLE IF EXISTS "{table}_staged" RENAME TO {table};'))

    replace.commit()

    print("\033[F", end="")
    print("Replacing old tables with staged... ✔")

    # -----------------
    # Remove old tables
    # -----------------

    print("\nDeleting temporary old tables...")
    delete = conn.begin()
    conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
    for table in target_tables:
        logging.debug(f"Dropping table {table}_old")
        conn.execute(text(f"DROP TABLE IF EXISTS {table}_old CASCADE;"))

    delete.commit()
    print("\033[F", end="")
    print("Deleting temporary old tables... ✔")

    # ----------------------
    # Rename _staged indexes
    # ----------------------

    print("\nRenaming indexes...")

    # sorted tables in the database
    db_meta.reflect(bind=db.Engine)

    # delete previous staged indexes
    delete_indexes = conn.begin()
    for meta_table in db_meta.sorted_tables:
        for index in meta_table.indexes:
            # remove the staged indexes
            if index.name and "_staged" in index.name:
                # conn.execute(text(schema.DropIndex(index)))
                renamed = index.name.replace("_staged", "")
                conn.execute(
                    text(f"ALTER INDEX IF EXISTS {index.name} RENAME TO {renamed};")
                )
        # primary key indexes are not listed under meta_table.indexes
        # so just rename these if they exist
        conn.execute(
            text(
                f"ALTER INDEX IF EXISTS pk_{meta_table.name}_staged RENAME TO pk_{meta_table.name};"
            )
        )
    delete_indexes.commit()

    print("\033[F", end="")
    print("Renaming indexes... ✔")

    # -------
    # Reindex
    # -------

    print("\nReindexing...")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute(
        text("REINDEX DATABASE postgres;")
    )
    print("\033[F", end="")
    print("Reindexing... ✔")

    # -------------
    # Final summary
    # -------------

    # Print row counts for each table.
    print("\n[Table Statistics]")
    with database.session_scope(db.Session) as db_session:
        with open(queries_dir / "table_sizes.sql") as file:
            SUMMARY_SQL = file.read()

        result = db_session.execute(text(SUMMARY_SQL))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

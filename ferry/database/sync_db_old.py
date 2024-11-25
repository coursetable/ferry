import pandas as pd
import csv
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect

from ferry import database
from ferry.database import Database, Base


queries_dir = Path(__file__).parent / "queries"


def sync_db_old(tables: dict[str, pd.DataFrame], database_connect_string: str):
    db = Database(database_connect_string)

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    with database.session_scope(db.Session) as db_session:
        print("Dropping all old objects...")
        with open(queries_dir / "drop_all.sql") as file:
            sql = file.read()
        db_session.execute(text(sql))
        print("\033[F", end="")
        print("Dropping all old objects... ✔")

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

    # Third step: create indexes
    # Reindexing cannot be done within a transaction
    print("\nReindexing...")
    conn = db.Engine.connect()
    conn.execution_options(isolation_level="AUTOCOMMIT")
    conn.execute(text("REINDEX SCHEMA public;"))
    conn.close()
    print("\033[F", end="")
    print("Reindexing... ✔")

    with database.session_scope(db.Session) as db_session:
        print("\nCreating metadata...")
        with open(queries_dir / "create_metadata.sql") as file:
            sql = file.read()
        db_session.execute(text(sql))
        print("\033[F", end="")
        print("Creating metadata... ✔")

        # TODO: we should set up some mechanism to automatically grant
        # privileges... The default on the schema is not enough.
        print("\nGranting privileges to hasura...")
        db_session.execute(text("GRANT SELECT ON ALL TABLES IN SCHEMA public TO hasura;"))
        db_session.execute(text("GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO hasura;"))
        print("\033[F", end="")
        print("Granting privileges to hasura... ✔")

        # Print row counts for each table.
        print("\n[Table Statistics]")
        with open(queries_dir / "table_sizes.sql") as file:
            sql = file.read()

        result = db_session.execute(text(sql))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

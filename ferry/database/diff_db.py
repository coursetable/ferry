# TODO load data into pandas

import pandas as pd
import csv
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect

from ferry import database
from ferry.database import Database, Base


queries_dir = Path(__file__).parent / "queries"


def get_dfs(database_connect_string: str):
    db = Database(database_connect_string)

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    conn = db.Engine.connect()
    
    # get table names
    query = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
    result = conn.execute(text(query))
    tables = [row[0] for row in result]

    # Initialize a dictionary to store DataFrames
    dataframes = {}

    # Load each table into a DataFrame
    for table_name in tables:
        df = pd.read_sql_table(table_name, con=conn)
        dataframes[table_name] = df

    print(dataframes["courses"])

def generate_diff(tables_old: dict[str, pd.DataFrame],
                    tables_new: dict[str, pd.DataFrame], output_dir:str):
    
    for table_name in tables_old.keys():
        if table_name not in tables_new.keys():
            raise ValueError(f"Table {table_name} not found in new tables")
        
        output_file_path = Path(output_dir).parent / (table_name + ".txt")

        with open(output_file_path, "w") as file:
             # check difference between old df and new df and output to above file path
            old_df = tables_old[table_name]
            new_df = tables_new[table_name]

            # check for rows that are in old df but not in new df
            missing_rows = old_df[~old_df.isin(new_df)].dropna()
            if not missing_rows.empty:
                file.write(f"Rows missing in new table: {missing_rows}\n")

            # check for rows that are in new df but not in old df
            new_rows = new_df[~new_df.isin(old_df)].dropna()
            if not new_rows.empty:
                file.write(f"New rows in new table: {new_rows}\n")

            # check for rows that have changed
            changed_rows = old_df[~old_df.eq(new_df)].dropna()
            if not changed_rows.empty:
                file.write(f"Changed rows in new table: {changed_rows}\n")

            # check for rows that have been deleted
            deleted_rows = new_df[~new_df.isin(old_df)].dropna()
            if not deleted_rows.empty:
                file.write(f"Deleted rows in new table: {deleted_rows}\n")
            


get_dfs("postgresql://postgres:postgres@db:5432/postgres")

def sync_db(tables: dict[str, pd.DataFrame], database_connect_string: str):
    db = Database(database_connect_string)

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # First step: existing -> old
    print("\nMoving existing tables...")

    conn = db.Engine.connect()
    inspector = inspect(db.Engine)
    
    # TODO: remove this; currently we have a deadlock issue when executing on prod DB
    conn.execution_options(isolation_level="AUTOCOMMIT")
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

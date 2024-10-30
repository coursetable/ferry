# TODO load data into pandas

import pandas as pd
import csv
import json
import ast
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect

from ferry import database
from ferry.crawler.classes.parse import normalize_unicode
from ferry.database import Database, Base
from ferry.transform import transform


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

    return dataframes

def check_change(row, table_name):
    cols_to_exclude = {
        "flags" : [],
        "course_flags" : [],
        "professors" : [],
        "course_professors" : [],
        'courses' : ['same_course_and_profs_id', 'same_course_id', 'same_prof_id', 'last_offered_course_id'],
        "listings" : [],
    }

    for col_name in row.index.tolist():
        if ("_old" not in col_name):
            continue
        col_name = col_name.replace("_old", "")
        
        if (col_name in cols_to_exclude[table_name]):
            return False
        
        old_value = row[col_name + "_old"]
        new_value = row[col_name + "_new"]
        
        if isinstance(old_value, list) or isinstance(new_value, list):
            old_value = ast.literal_eval(str(old_value).replace('"',"'")) # fix quotes
            new_value = ast.literal_eval(str(new_value).replace('"',"'"))
            if (old_value != new_value):
                return True
        else:
            if (not pd.isna(old_value) and
            not pd.isna(new_value)):
                if isinstance(old_value, dict) and isinstance(new_value, str):
                    new_value = json.loads(new_value)
                elif isinstance(old_value, (int, float)) and isinstance(new_value, (int, float)):
                    new_value = float(new_value)
                    old_value = float(old_value)
                else:
                
                    old_value = str(old_value).replace('"',"'").replace('\\', '').strip("'")
                    new_value = str(new_value).replace('"',"'").replace('\\', '').strip("'")
                    # old_value = normalize_unicode(old_value)
                    # new_value = normalize_unicode(new_value)
                    try:
                        old_value = json.loads(old_value)
                        new_value = json.loads(new_value)
                    except:
                        pass
                if old_value != new_value:
                    print(f"column: {col_name}, old: {old_value}, new: {new_value}")
                    return True
            
    return False

def generate_diff(tables_old: dict[str, pd.DataFrame],
                    tables_new: dict[str, pd.DataFrame], output_dir:str):
    
    primary_keys = {
        "flags" : ["flag_id"],
        "course_flags" : ["course_id"],
        "professors" : ["professor_id"],
        "course_professors" : ["course_id"],
        "courses" : ["course_id"],
        "listings" : ["listing_id"],
    }


    for table_name in primary_keys.keys():
        if table_name not in tables_new.keys() or table_name not in tables_old.keys():
            raise ValueError(f"Table {table_name} not found in new tables")
        
        print(f"Computing diff for table {table_name} ...", end=" ")
        
        output_file_path = Path(output_dir) / (table_name + ".txt")
        

        with open(output_file_path, "w+") as file:
             # check difference between old df and new df and output to above file path
            old_df = tables_old[table_name]
            new_df = tables_new[table_name]
            
            ## if want to test with differences between old and new df
            # if (table_name == "courses"):
            #     # temporary -- can be created by uncommenting where it says just testing
            #     old_df = pd.read_csv("/workspaces/ferry/new_df.csv")

            # TODO - better way to do this?
            pk = primary_keys[table_name][0]

            # Identify rows with differences
            
            # check for rows that are in old df but not in new df
            # based on primary key
            missing_rows = old_df[~old_df[pk].isin(new_df[pk])]
            if not missing_rows.empty:
                file.write(f"Rows missing in new table: {missing_rows.to_csv()}\n")
            
            # check for rows that have been deleted
            deleted_rows = new_df[~new_df[pk].isin(old_df[pk])]
            if not deleted_rows.empty:
                file.write(f"Deleted rows in new table: {deleted_rows.to_csv()}\n")

            # check for row

            # just testing
            # old_df.to_csv("old_df.csv", index=False)
            # new_df.to_csv("new_df.csv", index=False)
            
            # check for rows that have changed
            # changed_rows = old_df[((~old_df.isna()) & (~new_df.isna()) & (old_df != new_df)).any(axis=1)]
            
            # if not changed_rows.empty:
            #     file.write(f"Changed rows in new table: {changed_rows}\n")
            if table_name == "course_flags":
                old_df = old_df.groupby("course_id")["flag_id"].apply(frozenset)
                new_df = new_df.groupby("course_id")["flag_id"].apply(frozenset)
            elif table_name == "course_professors":
                old_df = old_df.groupby("course_id")["professor_id"].apply(frozenset)
                new_df = new_df.groupby("course_id")["professor_id"].apply(frozenset)

            
            merged_df = pd.merge(old_df, new_df, on=pk,
                                 how="inner", suffixes=('_old', '_new'))
            
            changed_rows = merged_df[merged_df.apply(check_change, args=(table_name,), axis=1)]
            
            if not changed_rows.empty:
                file.write(f"Changed rows in new table: {changed_rows.to_csv()}\n")
        
        print("✔")


tables_old = get_dfs("postgresql://postgres:postgres@db:5432/postgres")
tables_new = transform(data_dir=Path("/workspaces/ferry/data"))
generate_diff(tables_old, tables_new, "/workspaces/ferry/diff")

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

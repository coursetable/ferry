import pandas as pd
import numpy as np
import csv
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect, Connection
from psycopg2.extensions import register_adapter, AsIs

from ferry import database
from ferry.database import Database, Base
from .diff_db import get_tables_from_db, generate_diff, primary_keys

register_adapter(np.int64, AsIs)

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


queries_dir = Path(__file__).parent / "queries"


def commit_additions(
    table_name: str, to_add: pd.DataFrame, conn: Connection, db_meta: MetaData
):
    if len(to_add) == 0:
        return

    # Validate table_name
    valid_tables = set(db_meta.tables.keys())
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}")

    valid_columns = get_valid_columns(table_name, db_meta)
    print(f"Adding {len(to_add)} new rows to {table_name}")
    for _, row in to_add.iterrows():
        columns_list = list(row.index)
        columns_list.append("time_added")
        columns_list.append("last_updated")

        # Validate columns
        for col in columns_list:
            if col not in valid_columns:
                raise ValueError(f"Invalid column name: {col} in table {table_name}")

        columns = ", ".join(columns_list)

        # Build placeholders for the values
        placeholders = []
        params = {}
        for col in row.index:
            placeholders.append(f":{col}")
            val = row[col]
            if pd.isna(val) or val in [None, "None", "NULL", "<NA>", "nan"]:
                val = None
            params[col] = val

        placeholders.append("CURRENT_TIMESTAMP")
        placeholders.append("CURRENT_TIMESTAMP")

        values = ", ".join(placeholders)

        insert_query = text(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        conn.execute(insert_query, params)


def commit_deletions(
    table_name: str, to_remove: pd.DataFrame, conn: Connection, db_meta: MetaData
):
    pk = primary_keys[table_name][0]

    if len(to_remove) == 0:
        return
    print(f"Removing {len(to_remove)} rows from {table_name}")

    # Validate table_name
    valid_tables = set(db_meta.tables.keys())
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}")

    for _, row in to_remove.iterrows():
        delete_query = text(f"DELETE FROM {table_name} WHERE {pk} = :pk_value;")
        conn.execute(delete_query, {"pk_value": row[pk]})


def commit_updates(
    table_name: str, to_update: pd.DataFrame, conn: Connection, db_meta: MetaData
):
    if len(to_update) == 0:
        return
    pk = primary_keys[table_name][0]

    # Validate table_name
    valid_tables = set(db_meta.tables.keys())
    if table_name not in valid_tables:
        raise ValueError(f"Invalid table name: {table_name}")

    valid_columns = get_valid_columns(table_name, db_meta)
    for _, row in to_update.iterrows():
        if table_name == "course_meetings":
            # Delete the existing meetings with that course id
            delete_query = text(f"DELETE FROM {table_name} WHERE {pk} = :pk_value;")
            conn.execute(delete_query, {"pk_value": row[pk]})

            # Add the new meetings
            meetings = row["meetings_new"]
            meetings_old = row["meetings_old"]
            for i, meeting in enumerate(meetings):
                columns_list = list(meetings_old[i].keys())

                # Validate columns
                for col in columns_list:
                    if col not in valid_columns:
                        raise ValueError(
                            f"Invalid column name: {col} in table {table_name}"
                        )

                placeholders = []
                params = {}
                for col in columns_list:
                    if col == "last_updated":
                        placeholders.append("CURRENT_TIMESTAMP")
                    elif col == "time_added":
                        old_val = meetings_old[i]["time_added"]
                        if pd.isna(old_val) or old_val in [
                            None,
                            "None",
                            "NULL",
                            "<NA>",
                            "nan",
                        ]:
                            placeholders.append("CURRENT_TIMESTAMP")
                        else:
                            placeholders.append(f":{col}")
                            params[col] = old_val
                    else:
                        val = meeting[col]
                        if pd.isna(val) or val in [
                            None,
                            "None",
                            "NULL",
                            "<NA>",
                            "nan",
                        ]:
                            val = None

                        placeholders.append(f":{col}")
                        params[col] = val

                columns = ", ".join(columns_list)
                values = ", ".join(placeholders)
                insert_query = text(
                    f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                )
                conn.execute(insert_query, params)
        elif table_name == "course_flags":
            # Delete the existing flags with that course id
            delete_query = text(f"DELETE FROM {table_name} WHERE {pk} = :pk_value;")
            conn.execute(delete_query, {"pk_value": row[pk]})
            
            # Add the new flags
            flags = row["flag_id_new"] # frozenset
            flags_old = row["flag_id_old"] # frozenset
            if type(flags) == frozenset:
                flags = list(flags)
            if type(flags_old) == frozenset:
                flags_old = list(flags_old)

            for i, flag in enumerate(flags):
                # Validate columns
                for col in row.index:
                    if "_new" in col:
                        col_name_orig = col.replace("_new", "")
                    else:
                        continue
                    if col_name_orig not in valid_columns:
                        raise ValueError(
                            f"Invalid column name: {col} in table {table_name}"
                        )

                placeholders = []
                params = {}
                columns_list = []
                columns_list.append("last_updated")
                placeholders.append("CURRENT_TIMESTAMP")
                for col in row.index:
                    if "_new" in col:
                        col_name_orig = col.replace("_new", "")
                    elif col == "course_id":
                        col_name_orig = col
                    else:
                        continue

                    columns_list.append(col_name_orig)

                    val = row[col]
                    if col_name_orig == "flag_id":
                        val = flag
                    
                    if pd.isna(val) or val in [
                        None,
                        "None",
                        "NULL",
                        "<NA>",
                        "nan",
                    ]:
                        val = None

                    placeholders.append(f":{col_name_orig}")
                    params[col_name_orig] = val

                columns = ", ".join(columns_list)
                values = ", ".join(placeholders)
                insert_query = text(
                    f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                )
                conn.execute(insert_query, params)
        else:
            set_clause_items = []
            params = {}
            for col in row.index:
                if "_new" in col:
                    col_name_orig = col.replace("_new", "")
                else:
                    continue

                if col_name_orig not in valid_columns:
                    raise ValueError(
                        f"Invalid column name: {col_name_orig} in table {table_name}"
                    )

                val = row[col]
                if pd.isna(val) or val in [None, "None", "NULL", "<NA>", "nan"]:
                    val = None

                if val is not None:
                    set_clause_items.append(f"{col_name_orig} = :{col_name_orig}")
                    params[col_name_orig] = val
                else:
                    set_clause_items.append(f"{col_name_orig} = NULL")

            set_clause_items.append("last_updated = CURRENT_TIMESTAMP")

            set_clause = ", ".join(set_clause_items)
            where_clause = f"{pk} = :pk_value"
            params["pk_value"] = row[pk]

            update_query = text(
                f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
            )
            conn.execute(update_query, params)


def get_valid_columns(table_name, db_meta):
    table = db_meta.tables[table_name]
    return [col.name for col in table.columns]


def sync_db(tables: dict[str, pd.DataFrame], database_connect_string: str):
    db = Database(database_connect_string)

    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # Get valid table names
    valid_tables = set(db_meta.tables.keys())

    def get_valid_columns(table_name):
        if table_name not in valid_tables:
            raise ValueError(f"Invalid table name: {table_name}")
        table = db_meta.tables[table_name]
        return [col.name for col in table.columns]

    # Order to process tables to avoid foreign key constraint issues
    # We can't use db_meta.sorted_tables because that only has existing tables in DB
    tables_order_add = [
        "buildings",
        "locations",
        "flags",
        "professors",
        "courses",
        "course_flags",
        "course_meetings",
        "course_professors",
        "listings",
    ]
    tables_order_delete = tables_order_add[::-1]

    for table_name in tables_order_add:
        # Make sure all tables are in the database
        if table_name in db_meta.tables:
            continue
        conn_new = db.Engine.raw_connection()
        Base.metadata.create_all(db.Engine)
        if table_name not in tables:
            raise ValueError(f"There is no data for table {table_name}.")
        buffer = StringIO()
        tables[table_name] = tables[table_name].replace({r"\r": ""}, regex=True)
        tables[table_name].to_csv(
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
        cursor = conn_new.cursor()

        try:
            cursor.copy_from(
                buffer,
                table_name,
                columns=tables[table_name].columns,
                sep="\t",
                null="NULL",
            )
        except Exception as error:
            conn_new.rollback()
            cursor.close()
            raise error

        cursor.close()
        conn_new.commit()

    print("Generating diff...")
    tables_old = get_tables_from_db(database_connect_string)
    diff = generate_diff(tables_old, tables, "/workspaces/ferry/diff")

    conn = db.Engine.connect()
    inspector = inspect(db.Engine)
    # TODO: remove this; currently we have a deadlock issue when executing on prod DB
    conn.execution_options(isolation_level="AUTOCOMMIT")
    update = conn.begin()

    for table_name in tables_order_add:
        # Check if the table has columns 'last_updated' and 'time_added'
        columns = inspector.get_columns(table_name)
        has_last_updated = any(col["name"] == "last_updated" for col in columns)
        has_time_added = any(col["name"] == "time_added" for col in columns)

        if not has_time_added:
            print(f"adding new column time_added to {table_name}")
            conn.execute(
                text(
                    f"ALTER TABLE {table_name} ADD COLUMN time_added TIMESTAMP DEFAULT NULL;"
                )
            )
        if not has_last_updated:
            print(f"adding new column last_updated to {table_name}")
            conn.execute(
                text(
                    f"ALTER TABLE {table_name} ADD COLUMN last_updated TIMESTAMP DEFAULT NULL;"
                )
            )
    for table_name in tables_order_add:
        commit_additions(table_name, diff[table_name]["added_rows"], conn, db_meta)
        commit_updates(table_name, diff[table_name]["changed_rows"], conn, db_meta)
    for table_name in tables_order_delete:
        commit_deletions(table_name, diff[table_name]["deleted_rows"], conn, db_meta)
    update.commit()
    print("\033[F", end="")

    # Print row counts for each table.
    print("\n[Table Statistics]")
    with database.session_scope(db.Session) as db_session:
        with open(queries_dir / "table_sizes.sql") as file:
            SUMMARY_SQL = file.read()

        result = db_session.execute(text(SUMMARY_SQL))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

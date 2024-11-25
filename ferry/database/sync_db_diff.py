import pandas as pd
import csv
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect

from ferry import database
from ferry.database import Database, Base
from ferry.database import get_dfs, generate_diff, primary_keys
from ferry.transform import transform

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


queries_dir = Path(__file__).parent / "queries"


def sync_db(tables: dict[str, pd.DataFrame], database_connect_string: str):
    # this is the sync db function that will be called from main.py

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

    update = conn.begin()

    # order to process tables to avoid foreign key constraint issues
    tables_order_add = [
        "courses",
        "listings",
        "flags",
        "course_flags",
        "professors",
        "course_professors",
        "buildings",
        "locations",
        "course_meetings",
    ]

    # reverse order when deleting
    tables_order_delete = [
        "course_meetings",
        "locations",
        "buildings",
        "course_professors",
        "professors",
        "course_flags",
        "flags",
        "listings",
        "courses",
    ]

    for table_name in tables_order_add:
        # check if table exists in database
        if table_name not in db_meta.tables:
            conn_new = db.Engine.raw_connection()
            Base.metadata.create_all(db.Engine)
            if table_name not in tables:
                raise ValueError(f"There is no data for table {table_name}.")
            # create in-memory buffer for DataFrame
            buffer = StringIO()
            # TODO is this really needed?
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
            continue

    print("Generating diff...")
    tables_old = get_dfs(database_connect_string)
    diff = generate_diff(tables_old, tables, "/workspaces/ferry/diff")

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

        diffs = diff[table_name]
        print(f"Syncing (Add/Update) Table: {table_name}")

        # first add the new rows
        to_add = diffs["added_rows"]
        # add these rows to the database table
        if len(to_add) > 0:
            print(f"Adding {len(to_add)} new rows to {table_name}")
            for _, row in to_add.iterrows():
                columns_list = list(row.index)
                columns_list.append("time_added")
                columns_list.append("last_updated")
                columns = ", ".join(columns_list)

                values_list = []
                for col in row.index:
                    val = row[col]
                    if pd.isna(val) or val in [None, "None", "NULL", "<NA>", "nan"]:
                        val = None

                    values_list.append(val)

                values_list.append("NOW()")
                values_list.append("NOW()")
                values = ", ".join(
                    f"'{str(v)}'" if v is not None else "NULL" for v in values_list
                )

                insert_query = (
                    f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                )
                conn.execute(text(insert_query))

        pk = primary_keys[table_name][0]

        to_update = diffs["changed_rows"]
        # update these rows in the database table
        if len(to_update) > 0:
            for _, row in to_update.iterrows():

                if table_name == "course_meetings":
                    # delete the existing meetings with that course id
                    where_clause = f"{pk} = '{row[pk]}'"
                    delete_query = f"DELETE FROM {table_name} WHERE {where_clause};"
                    conn.execute(text(delete_query))

                    # add the new meetings
                    meetings = row["meetings_new"]
                    meetings_old = row["meetings_old"]
                    for i, meeting in enumerate(meetings):
                        columns_list = list(meetings_old[i].keys())

                        values_list = []
                        for col in columns_list:
                            val = ""
                            if col == "last_updated":
                                val = "NOW()"
                            elif col == "time_added":
                                old_val = meetings_old[i]["time_added"]
                                if pd.isna(old_val) or old_val in [
                                    None,
                                    "None",
                                    "NULL",
                                    "<NA>",
                                    "nan",
                                ]:
                                    val = "NOW()"
                                else:
                                    val = old_val
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

                            values_list.append(val)

                        values = ", ".join(
                            f"'{str(v)}'" if v is not None else "NULL"
                            for v in values_list
                        )

                        columns = ", ".join(columns_list)
                        insert_query = (
                            f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
                        )
                        conn.execute(text(insert_query))

                # TODO: check differences between specific columns again or update the whole row?
                # might have to create new function just for checking difference between two columns in a row
                else:
                    set_clause_items = []
                    for col in row.index:
                        col_name_orig = ""
                        if "_new" in col:
                            col_name_orig = col.replace("_new", "")
                        else:
                            continue

                        val = row[col]
                        if pd.isna(val) or val in [None, "None", "NULL", "<NA>", "nan"]:
                            val = None

                        if val is not None:
                            if isinstance(val, str):
                                val = val.replace("'", "''")
                            set_clause_items.append(f"{col_name_orig} = '{val}'")
                        else:
                            set_clause_items.append(f"{col_name_orig} = NULL")

                    set_clause_items.append("last_updated = NOW()")

                    set_clause = ", ".join(set_clause_items)
                    where_clause = f"{pk} = '{row[pk]}'"
                    update_query = (
                        f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
                    )
                    conn.execute(text(update_query))

    for table_name in tables_order_delete:
        diffs = diff[table_name]
        print(f"Syncing (Delete) Table: {table_name}")

        pk = primary_keys[table_name][0]

        to_remove = diffs["deleted_rows"]

        # remove these rows from the database table
        if len(to_remove) > 0:
            print(f"Removing {len(to_remove)} rows from {table_name}")

            for _, row in to_remove.iterrows():
                where_clause = f"{pk} = '{row[pk]}'"
                delete_query = f"DELETE FROM {table_name} WHERE {where_clause};"
                conn.execute(text(delete_query))

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

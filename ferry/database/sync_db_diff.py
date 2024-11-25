import pandas as pd
import csv
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect, Connection

from ferry import database
from ferry.database import Database, Base
from .diff_db import get_tables_from_db, generate_diff, primary_keys

logging.basicConfig(
    format="%(asctime)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)
logging.getLogger("sqlalchemy.engine").setLevel(logging.INFO)


queries_dir = Path(__file__).parent / "queries"


def commit_additions(table_name: str, to_add: pd.DataFrame, conn: Connection):
    if len(to_add) == 0:
        return
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

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({values});"
        conn.execute(text(insert_query))


def commit_deletions(table_name: str, to_remove: pd.DataFrame, conn: Connection):
    pk = primary_keys[table_name][0]

    if len(to_remove) == 0:
        return
    print(f"Removing {len(to_remove)} rows from {table_name}")

    for _, row in to_remove.iterrows():
        where_clause = f"{pk} = '{row[pk]}'"
        delete_query = f"DELETE FROM {table_name} WHERE {where_clause};"
        conn.execute(text(delete_query))


def commit_updates(table_name: str, to_update: pd.DataFrame, conn: Connection):
    if len(to_update) == 0:
        return
    pk = primary_keys[table_name][0]
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
                    f"'{str(v)}'" if v is not None else "NULL" for v in values_list
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
            update_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
            conn.execute(text(update_query))


def sync_db(tables: dict[str, pd.DataFrame], database_connect_string: str):
    db = Database(database_connect_string)

    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)
    # Order to process tables to avoid foreign key constraint issues
    tables_order_add = [
        t.name for t in db_meta.sorted_tables if t.name in primary_keys
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
        commit_additions(table_name, diff[table_name]["added_rows"], conn)
        commit_updates(table_name, diff[table_name]["changed_rows"], conn)
    for table_name in tables_order_delete:
        commit_deletions(table_name, diff[table_name]["deleted_rows"], conn)
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

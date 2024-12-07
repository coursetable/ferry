import pandas as pd
import numpy as np
import ujson
from pathlib import Path
import logging

from sqlalchemy import MetaData, text, inspect, Connection
from psycopg2.extensions import register_adapter, AsIs

from ferry import database
from ferry.database import Database
from .diff_db import get_tables_from_db, generate_diff, primary_keys

register_adapter(np.int64, AsIs)


queries_dir = Path(__file__).parent / "queries"

# Changes to these columns are synced to the DB, but are not recorded by last mod.
# These are purely computed columns and are subject to change based on our algorithm.
computed_columns = {
    "courses": [
        "same_course_id",
        "same_course_and_profs_id",
        "average_gut_rating",
        "average_professor_rating",
        "average_rating",
        "average_rating_n",
        "average_workload",
        "average_workload_n",
        "average_rating_same_professors",
        "average_rating_same_professors_n",
        "average_workload_same_professors",
        "average_workload_same_professors_n",
        "last_offered_course_id",
        "last_enrollment_course_id",
        "last_enrollment",
        "last_enrollment_season_code",
        "last_enrollment_same_professors",
    ],
    "listings": [],
    "course_professors": [],
    "professors": [
        "courses_taught",
        "average_rating",
        "average_rating_n",
    ],
    "course_flags": [],
    "flags": [],
    "course_meetings": [],
    "locations": [],
    "buildings": [],
}

# Junction tables do not have added/updated timestamps. Rather, any changes to
# them are recorded in the tables they connect.
junction_tables = {
    "course_professors": ["courses"],
    "course_flags": ["courses"],
    "course_meetings": ["courses"],
}


def commit_additions(table_name: str, to_add: pd.DataFrame, conn: Connection):
    if len(to_add) == 0:
        return

    logging.debug(f"Adding {len(to_add)} new rows to {table_name}")
    for _, row in to_add.iterrows():
        columns = ", ".join(row.index)
        if table_name not in junction_tables:
            columns = f"{columns}, time_added, last_updated"
        values = ", ".join(f":{col}" for col in row.index)
        if table_name not in junction_tables:
            values = f"{values}, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP"
        params = {col: row[col] if not pd.isna(row[col]) else None for col in row.index}

        insert_query = text(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        conn.execute(insert_query, params)

        if table_name in junction_tables:
            # Update the last_updated timestamp of the connected table
            # This assumes that the PK of the connected table is always present
            # in the junction table, which is indeed the case
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {col: row[col] for col in pk}

                update_query = text(
                    f"UPDATE {connected_table} SET last_updated = CURRENT_TIMESTAMP WHERE {where_clause};"
                )
                conn.execute(update_query, params)


def commit_deletions(table_name: str, to_remove: pd.DataFrame, conn: Connection):
    if len(to_remove) == 0:
        return
    logging.debug(f"Removing {len(to_remove)} rows from {table_name}")
    pk = primary_keys[table_name]
    for _, row in to_remove.iterrows():
        where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
        params = {col: row[col] for col in pk}

        delete_query = text(f"DELETE FROM {table_name} WHERE {where_clause};")
        conn.execute(delete_query, params)

        if table_name in junction_tables:
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {col: row[col] for col in pk}

                update_query = text(
                    f"UPDATE {connected_table} SET last_updated = CURRENT_TIMESTAMP WHERE {where_clause};"
                )
                conn.execute(update_query, params)


def commit_updates(table_name: str, to_update: pd.DataFrame, conn: Connection):
    if len(to_update) == 0:
        return
    logging.debug(f"Updating {len(to_update)} rows from {table_name}")
    pk = primary_keys[table_name]
    for _, row in to_update.iterrows():
        columns_changed = row["columns_changed"]
        row = row.drop("columns_changed")
        where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
        set_clause = ", ".join(f"{col} = :{col}" for col in row.index)
        # Only update last_updated if one of the changed columns is not computed
        if table_name not in junction_tables and not (
            set(columns_changed) <= set(computed_columns[table_name])
        ):
            set_clause = f"{set_clause}, last_updated = CURRENT_TIMESTAMP"
        params = {
            **{col: row[col] if not pd.isna(row[col]) else None for col in row.index},
            **{col: row[col] if not pd.isna(row[col]) else None for col in pk},
        }

        update_query = text(
            f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
        )
        conn.execute(update_query, params)

        # Note! This assumes junction tables do not have computed columns.
        # This is a fine assumption for now.
        if table_name in junction_tables:
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {col: row[col] for col in pk}

                update_query = text(
                    f"UPDATE {connected_table} SET last_updated = CURRENT_TIMESTAMP WHERE {where_clause};"
                )
                conn.execute(update_query, params)


def sync_db(
    tables: dict[str, pd.DataFrame], database_connect_string: str, data_dir: Path
):
    db = Database(database_connect_string)

    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # Order to process tables to avoid foreign key constraint issues
    tables_order_add = [table.name for table in db_meta.sorted_tables]
    tables_order_delete = tables_order_add[::-1]
    # Make sure all tables are in the database
    nonexistent_tables = set(primary_keys.keys()) - set(tables_order_add)
    if nonexistent_tables:
        raise LookupError(
            f"Tables {nonexistent_tables} not found in database. Run with --rewrite once to create the tables."
        )

    print("Generating diff...")
    tables_old = get_tables_from_db(database_connect_string)
    # Pandas would read JSON columns as real values, so we serialize them again
    tables_old["courses"]["skills"] = tables_old["courses"]["skills"].apply(ujson.dumps)
    tables_old["courses"]["areas"] = tables_old["courses"]["areas"].apply(ujson.dumps)
    tables_old["course_meetings"]["location_id"] = tables_old["course_meetings"][
        "location_id"
    ].astype(pd.Int64Dtype())
    diff = generate_diff(tables_old, tables, data_dir / "diff")

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
            conn.execute(
                text(
                    f"ALTER TABLE {table_name} ADD COLUMN time_added TIMESTAMP DEFAULT NULL;"
                )
            )
        if not has_last_updated:
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

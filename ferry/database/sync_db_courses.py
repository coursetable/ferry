import pandas as pd
import numpy as np
import ujson
from pathlib import Path
import logging
from typing import Any, Dict, Union

from sqlalchemy import MetaData, text, inspect, Connection
from psycopg2.extensions import register_adapter, AsIs

from ferry.database import Database
from .generate_changelog import print_diff, computed_columns, primary_keys, DiffRecord


register_adapter(np.int64, AsIs)


queries_dir = Path(__file__).parent / "queries"

# Junction tables do not have added/updated timestamps. Rather, any changes to
# them are recorded in the tables they connect.
junction_tables = {
    "course_professors": ["courses"],
    "course_flags": ["courses"],
    "course_meetings": ["courses"],
}


def get_tables_from_db(database_connect_string: str) -> dict[str, pd.DataFrame]:
    db = Database(database_connect_string)
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)
    conn = db.Engine.connect()

    return {
        table_name: pd.read_sql_table(table_name, con=conn).drop(
            columns=["time_added", "last_updated"], errors="ignore"
        )
        for table_name in primary_keys.keys()
    }


def generate_diff(
    tables_old: dict[str, pd.DataFrame], tables_new: dict[str, pd.DataFrame]
):
    diff_dict: dict[str, DiffRecord] = {}

    for table_name in primary_keys.keys():
        if table_name not in tables_new.keys():
            raise ValueError(f"Table '{table_name}' not found in new tables")
        if table_name not in tables_old.keys():
            raise ValueError(f"Table '{table_name}' not found in old tables")

        print(f"Computing diff for table {table_name} ...", end=" ")

        old_df = tables_old[table_name]
        new_df = tables_new[table_name]

        pk = primary_keys[table_name]
        old_df = old_df.set_index(pk)
        new_df = new_df.set_index(pk)

        deleted_rows = old_df[~old_df.index.isin(new_df.index)]
        added_rows = new_df[~new_df.index.isin(old_df.index)]

        # Must sort index in order to compare dataframes cell-wise
        shared_rows_old = (
            old_df[old_df.index.isin(new_df.index)].sort_index().sort_index(axis=1)
        )
        shared_rows_new = (
            new_df[new_df.index.isin(old_df.index)].sort_index().sort_index(axis=1)
        )
        if (shared_rows_old.index != shared_rows_new.index).any():
            print(shared_rows_old.index)
            print(shared_rows_new.index)
            raise ValueError(f"Unexpected: index mismatch in table {table_name}")
        if (
            len(shared_rows_old.columns) != len(shared_rows_new.columns)
            or (shared_rows_old.columns != shared_rows_new.columns).any()
        ):
            print(shared_rows_old.columns)
            print(shared_rows_new.columns)
            raise ValueError(
                f"Column mismatch in table {table_name}. Run with --rewrite once to fix."
            )
        # Do not allow type changes unless one of them is NA
        old_types = shared_rows_old.map(type)
        new_types = shared_rows_new.map(type)
        different_types = ~(
            (old_types == new_types) | shared_rows_old.isna() | shared_rows_new.isna()
        )
        if different_types.any().any():
            row, col = list(zip(*different_types.values.nonzero()))[0]
            print(
                f"Type mismatch in {table_name} at ({row}, {col}) (column {shared_rows_old.columns[col]})"
            )
            print(f"Old type: {old_types.iat[row, col]}")
            print(f"New type: {new_types.iat[row, col]}")
            print(f"Old value: {shared_rows_old.iat[row, col]}")
            print(f"New value: {shared_rows_new.iat[row, col]}")
            raise TypeError("Type mismatch")
        unequal_mask = ~(
            (shared_rows_old == shared_rows_new)
            | (shared_rows_old.isna() & shared_rows_new.isna())
        )

        changed_rows = shared_rows_new[unequal_mask.any(axis=1)].copy()
        if len(changed_rows) > 0:
            changed_rows["columns_changed"] = unequal_mask[
                unequal_mask.any(axis=1)
            ].apply(lambda row: shared_rows_new.columns[row].tolist(), axis=1)
            # Add old values for primary key columns that are being changed
            for pk_col in pk:
                if pk_col in changed_rows.columns:
                    old_values = shared_rows_old[unequal_mask.any(axis=1)][pk_col]
                    changed_rows[f"old_{pk_col}"] = old_values
        else:
            changed_rows["columns_changed"] = pd.Series()

        diff_dict[table_name] = {
            "deleted_rows": deleted_rows.reset_index(),
            "added_rows": added_rows.reset_index(),
            "changed_rows": changed_rows.reset_index(),
        }

        print("✔")

    return diff_dict


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
        
        # Convert pandas values to Python types for SQL parameters
        params: Dict[str, Any] = {}
        for col in row.index:
            value = row[col]
            if pd.isna(value).item():
                params[str(col)] = None
            else:
                params[str(col)] = value

        insert_query = text(f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        conn.execute(insert_query, params)

        if table_name in junction_tables:
            # Update the last_updated timestamp of the connected table
            # This assumes that the PK of the connected table is always present
            # in the junction table, which is indeed the case
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {}
                for col in pk:
                    value = row[col]
                    if pd.isna(value).item():
                        params[str(col)] = None
                    else:
                        params[str(col)] = value

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
        where_conditions = []
        params: Dict[str, Any] = {}
        for col in pk:
            value = row[col]
            if pd.isna(value).item():
                params[str(col)] = None
                where_conditions.append(f"{col} IS NULL")
            else:
                params[str(col)] = value
                where_conditions.append(f"{col} = :{col}")
        where_clause = " AND ".join(where_conditions)

        delete_query = text(f"DELETE FROM {table_name} WHERE {where_clause};")
        conn.execute(delete_query, params)

        if table_name in junction_tables:
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {}
                for col in pk:
                    value = row[col]
                    if pd.isna(value).item():
                        params[str(col)] = None
                    else:
                        params[str(col)] = value

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
        
        # For course_meetings table, use old primary key values in WHERE clause
        if table_name == "course_meetings":
            # Use old values for primary key columns in WHERE clause
            where_conditions = []
            where_params: Dict[str, Any] = {}
            for pk_col in pk:
                old_col = f"old_{pk_col}"
                if old_col in row.index:
                    # Use old value for WHERE clause
                    where_conditions.append(f"{pk_col} = :{pk_col}")
                    value = row[old_col]
                    if pd.isna(value).item():
                        where_params[pk_col] = None
                    else:
                        where_params[pk_col] = value
                else:
                    # Use new value if no old value available
                    where_conditions.append(f"{pk_col} = :{pk_col}")
                    value = row[pk_col]
                    if pd.isna(value).item():
                        where_params[pk_col] = None
                    else:
                        where_params[pk_col] = value
            
            where_clause = " AND ".join(where_conditions)
            set_clause = ", ".join(f"{col} = :{col}" for col in row.index if not str(col).startswith("old_"))
            
            update_params: Dict[str, Any] = {}
            for col in row.index:
                if not str(col).startswith("old_"):
                    value = row[col]
                    if pd.isna(value).item():
                        update_params[str(col)] = None
                    else:
                        update_params[str(col)] = value
            update_params.update(where_params)

            update_query = text(
                f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
            )
            conn.execute(update_query, update_params)
        else:
            # Standard update logic for other tables
            where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
            set_clause = ", ".join(f"{col} = :{col}" for col in row.index)
            # Only update last_updated if one of the changed columns is not computed
            if table_name not in junction_tables and not (
                set(columns_changed) <= set(computed_columns[table_name])
            ):
                set_clause = f"{set_clause}, last_updated = CURRENT_TIMESTAMP"
            
            params: Dict[str, Any] = {}
            for col in row.index:
                value = row[col]
                if pd.isna(value).item():
                    params[str(col)] = None
                else:
                    params[str(col)] = value
            for col in pk:
                value = row[col]
                if pd.isna(value).item():
                    params[str(col)] = None
                else:
                    params[str(col)] = value

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
                params = {}
                for col in pk:
                    value = row[col]
                    if pd.isna(value).item():
                        params[str(col)] = None
                    else:
                        params[str(col)] = value

                update_query = text(
                    f"UPDATE {connected_table} SET last_updated = CURRENT_TIMESTAMP WHERE {where_clause};"
                )
                conn.execute(update_query, params)


def sync_locations_with_upsert(locations_df: pd.DataFrame, conn: Connection) -> dict:
    """Sync locations by introspecting DB for existing IDs, inserting new ones, and returning actual DB IDs."""
    
    location_mapping = {}
    
    for _, location in locations_df.iterrows():
        building_code = location['building_code']
        room = location['room'] if not pd.isna(location['room']).item() else None
        
        # Check if location already exists
        result = conn.execute(text("""
            SELECT location_id FROM locations 
            WHERE building_code = :building_code AND room = :room
        """), {
            'building_code': building_code,
            'room': room
        }).fetchone()
        
        if result:
            # Use existing ID
            location_id = result[0]
        else:
            # Insert new location and get returned ID
            result = conn.execute(text("""
                INSERT INTO locations (building_code, room) 
                VALUES (:building_code, :room)
                RETURNING location_id
            """), {
                'building_code': building_code,
                'room': room
            }).fetchone()
            if result is None:
                raise ValueError("Failed to insert location and get location_id")
            location_id = result[0]
        
        location_mapping[(building_code, room)] = location_id
    
    return location_mapping


def sync_db_courses(
    tables: dict[str, pd.DataFrame], database_connect_string: str, data_dir: Path
):
    db = Database(database_connect_string)

    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # Order to process tables to avoid foreign key constraint issues
    tables_order_add = [
        table.name for table in db_meta.sorted_tables if table.name in primary_keys
    ]
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
    tables_old["courses"]["primary_crn"] = tables_old["courses"]["primary_crn"].astype(
        pd.Int64Dtype()
    )
    tables_old["course_meetings"]["location_id"] = tables_old["course_meetings"][
        "location_id"
    ].astype(pd.Int64Dtype())
    diff = generate_diff(tables_old, tables)
    print_diff(diff, tables_old, tables, data_dir / "change_log")

    inspector = inspect(db.Engine)
    with db.Engine.begin() as conn:
        for table_name in tables_order_add:
            # Check if the table has columns 'last_updated' and 'time_added'
            if table_name in junction_tables:
                continue
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
        # Handle locations separately with UPSERT
        location_mapping = sync_locations_with_upsert(tables["locations"], conn)
        
        # Handle other tables normally
        for table_name in tables_order_add:
            if table_name != "locations":  # Skip locations since we handled it above
                commit_additions(table_name, diff[table_name]["added_rows"], conn)
                commit_updates(table_name, diff[table_name]["changed_rows"], conn)
        
        # Update course_meetings with correct location_ids AFTER they've been inserted
        if location_mapping:
            # Update the course_meetings table to use actual location_ids
            for (building_code, room), location_id in location_mapping.items():
                conn.execute(text("""
                    UPDATE course_meetings 
                    SET location_id = :location_id 
                    WHERE location_id IS NULL 
                    AND EXISTS (
                        SELECT 1 FROM locations 
                        WHERE building_code = :building_code 
                        AND room = :room
                    )
                """), {
                    'location_id': location_id,
                    'building_code': building_code,
                    'room': room
                })
        for table_name in tables_order_delete:
            if table_name != "locations":  # Skip locations deletion since we use UPSERT
                commit_deletions(table_name, diff[table_name]["deleted_rows"], conn)
        print("\033[F", end="")

    # Print row counts for each table.
    print("\n[Table Statistics]")
    with db.Engine.begin() as conn:
        with open(queries_dir / "table_sizes.sql") as file:
            SUMMARY_SQL = file.read()

        result = conn.execute(text(SUMMARY_SQL))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

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


def safe_isna(value) -> bool:
    """
    Safely check if a value is NA, handling both pandas scalars and Python scalars.
    """
    try:
        result = pd.isna(value)
        if hasattr(result, 'item'):
            return result.item()
        return bool(result)
    except (TypeError, ValueError):
        return value is None or (isinstance(value, float) and np.isnan(value))


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

    # only process tables that exist in both old and new tables
    tables_to_process = set(tables_old.keys()) & set(
        tables_new.keys()) & set(primary_keys.keys())

    for table_name in tables_to_process:
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
            old_df[old_df.index.isin(new_df.index)
                   ].sort_index().sort_index(axis=1)
        )
        shared_rows_new = (
            new_df[new_df.index.isin(old_df.index)
                   ].sort_index().sort_index(axis=1)
        )
        if (shared_rows_old.index != shared_rows_new.index).any():
            print(shared_rows_old.index)
            print(shared_rows_new.index)
            raise ValueError(
                f"Unexpected: index mismatch in table {table_name}")
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
        else:
            changed_rows["columns_changed"] = pd.Series(dtype=object)

        diff_dict[table_name] = {
            "deleted_rows": pd.DataFrame(deleted_rows).reset_index(),
            "added_rows": pd.DataFrame(added_rows).reset_index(),
            "changed_rows": pd.DataFrame(changed_rows).reset_index(),
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

        params = {str(col): row[col] if not safe_isna(
            row[col]) else None for col in row.index}

        insert_query = text(
            f"INSERT INTO {table_name} ({columns}) VALUES ({values});")
        conn.execute(insert_query, params)

        if table_name in junction_tables:
            # Update the last_updated timestamp of the connected table
            # This assumes that the PK of the connected table is always present
            # in the junction table, which is indeed the case
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {
                    str(col): row[col] if not safe_isna(row[col]) else None for col in pk
                }

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
        params = {}
        for col in pk:
            value = row[col] if not safe_isna(row[col]) else None
            params[str(col)] = value
            where_conditions.append(
                f"{col} IS NULL" if value is None else f"{col} = :{col}"
            )
        where_clause = " AND ".join(where_conditions)

        delete_query = text(f"DELETE FROM {table_name} WHERE {where_clause};")
        conn.execute(delete_query, params)

        if table_name in junction_tables:
            for connected_table in junction_tables[table_name]:
                pk = primary_keys[connected_table]
                where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
                params = {}
                for col in pk:
                    value = row[col] if not safe_isna(row[col]) else None
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

        if table_name == "course_meetings":
            where_conditions = []
            where_params: Dict[str, Any] = {}
            for pk_col in pk:
                old_col = f"old_{pk_col}"
                if old_col in row.index:
                    where_conditions.append(f"{pk_col} = :{pk_col}")
                    value = row[old_col] if not safe_isna(
                        row[old_col]) else None
                    where_params[pk_col] = value
                else:
                    where_conditions.append(f"{pk_col} = :{pk_col}")
                    value = row[pk_col] if not safe_isna(row[pk_col]) else None
                    where_params[pk_col] = value

            where_clause = " AND ".join(where_conditions)
            set_clause = ", ".join(
                f"{col} = :{col}" for col in row.index if not str(col).startswith("old_"))

            update_params: Dict[str, Any] = {}
            for col in row.index:
                if not str(col).startswith("old_"):
                    value = row[col] if not safe_isna(row[col]) else None
                    update_params[str(col)] = value
            update_params.update(where_params)

            update_query = text(
                f"UPDATE {table_name} SET {set_clause} WHERE {where_clause};"
            )
            conn.execute(update_query, update_params)
        else:
            where_clause = " AND ".join(f"{col} = :{col}" for col in pk)
            set_clause = ", ".join(f"{col} = :{col}" for col in row.index)
            if table_name not in junction_tables and not (
                set(columns_changed) <= set(computed_columns[table_name])
            ):
                set_clause = f"{set_clause}, last_updated = CURRENT_TIMESTAMP"

            params: Dict[str, Any] = {}
            for col in row.index:
                value = row[col] if not safe_isna(row[col]) else None
                params[str(col)] = value
            for col in pk:
                value = row[col] if not safe_isna(row[col]) else None
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
                params = {str(col): row[col] if not safe_isna(
                    row[col]) else None for col in pk}

                update_query = text(
                    f"UPDATE {connected_table} SET last_updated = CURRENT_TIMESTAMP WHERE {where_clause};"
                )
                conn.execute(update_query, params)


def upsert_locations(locations_df: pd.DataFrame, conn: Connection) -> dict[tuple, int]:
    """
    Upsert locations using postgres ON CONFLICT UPDATE.
    Returns a mapping from (building_code, room) to location_id.
    """
    location_mapping = {}

    for _, location in locations_df.iterrows():
        building_code = location['building_code']
        room = location['room'] if not safe_isna(location['room']) else None

        # Skip locations with invalid building_code (database constraint violation)
        if safe_isna(building_code) or building_code is None:
            continue

        # Use postgres UPSERT with ON CONFLICT UPDATE
        result = conn.execute(text("""
            INSERT INTO locations (building_code, room, time_added, last_updated) 
            VALUES (:building_code, :room, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (building_code, room) 
            DO UPDATE SET last_updated = CURRENT_TIMESTAMP
            RETURNING location_id
        """), {
            'building_code': building_code,
            'room': room,
        }).fetchone()

        if result is None:
            raise ValueError("Failed to upsert location and get location_id")
        location_id = result[0]

        # Store the mapping
        location_mapping[(building_code, room)] = location_id

    return location_mapping


def cleanup_dependencies_for_buildings(buildings_to_delete: pd.DataFrame, conn: Connection):
    """
    Handle dependencies for buildings that are about to be deleted.
    course_meetings → locations → buildings
    """
    if len(buildings_to_delete) == 0:
        logging.info("No buildings to delete, skipping dependency cleanup")
        return

    # Get list of building codes that will be deleted
    building_codes_to_delete = buildings_to_delete['code'].tolist()
    logging.info(
        f"About to clean up dependencies for buildings: {building_codes_to_delete}")

    placeholders = ', '.join(
        [f':code_{i}' for i in range(len(building_codes_to_delete))])
    params = {f'code_{i}': code for i,
              code in enumerate(building_codes_to_delete)}

    logging.info(
        "Step 1: Deleting course_meetings referencing affected locations (will be recreated by UPSERT)")
    meetings_result = conn.execute(text(f"""
        DELETE FROM course_meetings 
        WHERE location_id IN (
            SELECT location_id FROM locations 
            WHERE building_code IN ({placeholders})
        )
    """), params)

    meetings_deleted = meetings_result.rowcount
    logging.info(
        f"Deleted {meetings_deleted} course_meeting(s) referencing affected locations (will be recreated by UPSERT)")

    logging.info(
        "Step 2: Cleaning up locations referencing buildings to be deleted")
    locations_result = conn.execute(text(f"""
        DELETE FROM locations 
        WHERE building_code IN ({placeholders})
    """), params)

    locations_deleted = locations_result.rowcount
    logging.info(
        f"Cleaned up {locations_deleted} location(s) referencing buildings to be deleted: {building_codes_to_delete}")

    logging.info(
        f"Total cleanup: {meetings_deleted} meetings deleted + {locations_deleted} locations deleted for {len(building_codes_to_delete)} buildings")


def sync_course_meetings_incremental(
    old_course_meetings: pd.DataFrame,
    new_course_meetings: pd.DataFrame,
    conn: Connection
):
    """
    Incrementally sync course_meetings. Drops all meetings for courses that have changed, then recreates them.
    """
    logging.info("Performing incremental sync of course_meetings...")

    # Group both old and new by course_id to compare meeting sets
    old_grouped = old_course_meetings.groupby('course_id')
    new_grouped = new_course_meetings.groupby('course_id')

    old_course_ids = set(old_grouped.groups.keys())
    new_course_ids = set(new_grouped.groups.keys())

    changed_course_ids = set()

    # Check courses that exist in both old and new
    for course_id in old_course_ids & new_course_ids:
        old_subset = old_grouped.get_group(
            course_id).drop(columns=['course_id'])
        new_subset = new_grouped.get_group(
            course_id).drop(columns=['course_id'])

        # Sort both DFs for comparison
        old_meetings = old_subset.iloc[:].sort_values(
            ['start_time', 'end_time', 'location_id']).reset_index(drop=True)
        new_meetings = new_subset.iloc[:].sort_values(
            ['start_time', 'end_time', 'location_id']).reset_index(drop=True)

        # Compare the meeting sets (excluding course_id)
        try:
            if not old_meetings.equals(new_meetings):
                changed_course_ids.add(course_id)
        except (ValueError, TypeError):
            # If comparison fails due to type/structure differences, consider it changed
            changed_course_ids.add(course_id)

    # Courses only in new data (additions)
    added_course_ids = new_course_ids - old_course_ids
    changed_course_ids.update(added_course_ids)

    # Courses only in old data (removals) - these will be handled by deletion
    removed_course_ids = old_course_ids - new_course_ids
    changed_course_ids.update(removed_course_ids)

    if not changed_course_ids:
        logging.info("No course meetings changes detected")
        return

    logging.info(
        f"Detected {len(changed_course_ids)} courses with meeting changes")

    # Delete all meetings for changed courses
    course_ids_list = list(changed_course_ids)
    if course_ids_list:
        course_ids_str = ','.join(str(cid) for cid in course_ids_list)

        delete_result = conn.execute(text(f"""
            DELETE FROM course_meetings 
            WHERE course_id IN ({course_ids_str})
        """))

        logging.info(
            f"Deleted {delete_result.rowcount} existing course_meetings for {len(course_ids_list)} courses")

    # Insert new meetings for courses
    courses_to_insert = [
        cid for cid in changed_course_ids if cid in new_course_ids]
    if courses_to_insert:
        meetings_to_insert = new_course_meetings[
            new_course_meetings['course_id'].isin(courses_to_insert)
        ]

        if len(meetings_to_insert) > 0:
            # Handle duplicate meetings: prefer meetings with location over those without
            meetings_to_insert = meetings_to_insert.iloc[:].sort_values([
                                                                        'location_id'])

            meetings_to_insert = meetings_to_insert.drop_duplicates(
                subset=['course_id', 'start_time', 'end_time'],
                keep='first'
            )

            logging.info(
                f"After deduplication: {len(meetings_to_insert)} meetings to insert")
            # Batch insert using individual parameterized queries for safety
            inserted_count = 0
            for _, meeting in meetings_to_insert.iterrows():
                course_id = meeting['course_id']
                start_time = meeting['start_time']
                end_time = meeting['end_time']
                days_of_week = meeting['days_of_week']
                location_id = meeting['location_id'] if not safe_isna(
                    meeting['location_id']) else None

                conn.execute(text("""
                    INSERT INTO course_meetings (course_id, start_time, end_time, days_of_week, location_id)
                    VALUES (:course_id, :start_time, :end_time, :days_of_week, :location_id)
                """), {
                    'course_id': course_id,
                    'start_time': start_time,
                    'end_time': end_time,
                    'days_of_week': days_of_week,
                    'location_id': location_id,
                })
                inserted_count += 1

            logging.info(f"Inserted {inserted_count} new course_meetings")

    logging.info("Course meetings incremental sync completed")


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
    tables_old["courses"]["skills"] = tables_old["courses"]["skills"].apply(
        ujson.dumps)
    tables_old["courses"]["areas"] = tables_old["courses"]["areas"].apply(
        ujson.dumps)
    tables_old["courses"]["primary_crn"] = tables_old["courses"]["primary_crn"].astype(
        pd.Int64Dtype()
    )
    tables_old["course_meetings"]["location_id"] = tables_old["course_meetings"][
        "location_id"
    ].astype(pd.Int64Dtype())

    # Exclude course_meetings from diff computation
    tables_for_diff = {k: v for k, v in tables.items() if k not in [
        "course_meetings"]}
    tables_old_for_diff = {k: v for k, v in tables_old.items() if k not in [
        "course_meetings"]}

    diff = generate_diff(tables_old_for_diff, tables_for_diff)

    print_diff(diff, tables_old, tables, data_dir / "change_log")

    inspector = inspect(db.Engine)
    with db.Engine.begin() as conn:
        for table_name in tables_order_add:
            # Check if the table has columns 'last_updated' and 'time_added'
            if table_name in junction_tables:
                continue
            columns = inspector.get_columns(table_name)
            has_last_updated = any(
                col["name"] == "last_updated" for col in columns)
            has_time_added = any(
                col["name"] == "time_added" for col in columns)

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

        # Handle locations with UPSERT first
        location_mapping = upsert_locations(tables["locations"], conn)

        for table_name in tables_order_add:
            if table_name in ["locations", "course_meetings"]:
                continue
            commit_additions(table_name, diff[table_name]["added_rows"], conn)
            commit_updates(table_name, diff[table_name]["changed_rows"], conn)

        # Handle course_meetings with incremental drop-and-recreate approach
        # Must go after all other tables are added due to course id foreign key constraint
        if "course_meetings" in tables:
            # First resolve location IDs for new course_meetings
            course_meetings_with_locations = tables["course_meetings"].copy()
            for i, meeting in course_meetings_with_locations.iterrows():
                if meeting['location_id'] is None and '_building_code' in meeting.index and '_room' in meeting.index:
                    building_code = meeting['_building_code']
                    room = meeting['_room'] if not safe_isna(
                        meeting['_room']) else None
                    location_key = (building_code, room)

                    # Only update if the location exists in our mapping
                    # (locations with None building_code are skipped)
                    if location_key in location_mapping:
                        course_meetings_with_locations.at[i,
                                                          'location_id'] = location_mapping[location_key]
                    else:
                        if building_code is None or safe_isna(building_code):
                            logging.warning(
                                f"Cannot resolve location for course meeting due to None building_code: room='{room}'")
                        else:
                            logging.warning(
                                f"Location not found in mapping: building_code='{building_code}', room='{room}'")

            # Clean up temporary columns
            course_meetings_clean = course_meetings_with_locations.drop(
                columns=["_building_code", "_room"], errors="ignore")

            sync_course_meetings_incremental(
                tables_old["course_meetings"],
                course_meetings_clean,
                conn
            )

        for table_name in tables_order_delete:
            # skip deleting courses due to self-fk constraint
            if table_name in ["locations", "course_meetings", "courses"]:
                continue

            deleted_rows = diff[table_name]["deleted_rows"]
            logging.info(
                f"Processing deletions for table '{table_name}': {len(deleted_rows)} rows to delete")

            if table_name == "buildings":
                logging.info(
                    f"About to clean up dependencies before deleting {len(deleted_rows)} buildings")
                cleanup_dependencies_for_buildings(deleted_rows, conn)

            commit_deletions(table_name, deleted_rows, conn)
        print("\033[F", end="")

    # Print row counts for each table.
    print("\n[Table Statistics]")
    with db.Engine.begin() as conn:
        with open(queries_dir / "table_sizes.sql") as file:
            SUMMARY_SQL = file.read()

        result = conn.execute(text(SUMMARY_SQL))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

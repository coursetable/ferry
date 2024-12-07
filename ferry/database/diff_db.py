import pandas as pd
from pathlib import Path
from typing import TypedDict

from sqlalchemy import MetaData

from ferry.database import Database


queries_dir = Path(__file__).parent / "queries"

# The keys need to be in sync with the CourseTables class in import_courses.py
primary_keys = {
    "courses": ["course_id"],
    "listings": ["listing_id"],
    "course_professors": ["course_id", "professor_id"],
    "professors": ["professor_id"],
    "course_flags": ["course_id", "flag_id"],
    "flags": ["flag_id"],
    "course_meetings": ["course_id", "location_id", "start_time", "end_time"],
    "locations": ["location_id"],
    "buildings": ["code"],
}


def get_tables_from_db(database_connect_string: str) -> dict[str, pd.DataFrame]:
    db = Database(database_connect_string)
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)
    conn = db.Engine.connect()

    return {
        table_name: pd.read_sql_table(table_name, con=conn).drop(
            ["time_added", "last_updated"], errors="ignore"
        )
        for table_name in primary_keys.keys()
    }


class DiffRecord(TypedDict):
    deleted_rows: pd.DataFrame
    added_rows: pd.DataFrame
    # Has one extra column: columns_changed
    changed_rows: pd.DataFrame


def type_or_na(value):
    if pd.isna(value):
        return "None"
    return type(value)


def generate_diff(
    tables_old: dict[str, pd.DataFrame],
    tables_new: dict[str, pd.DataFrame],
    output_dir: Path,
):
    diff_dict: dict[str, DiffRecord] = {}

    # Make sure dif output directory exists
    output_dir.mkdir(parents=True, exist_ok=True)

    for table_name in primary_keys.keys():
        if table_name not in tables_new.keys():
            raise ValueError(f"Table '{table_name}' not found in new tables")
        if table_name not in tables_old.keys():
            raise ValueError(f"Table '{table_name}' not found in old tables")

        print(f"Computing diff for table {table_name} ...", end=" ")

        output_file_path = output_dir / (table_name + ".md")

        with open(output_file_path, "w+") as file:
            old_df = tables_old[table_name]
            new_df = tables_new[table_name]

            pk = primary_keys[table_name]
            old_df = old_df.set_index(pk)
            new_df = new_df.set_index(pk)

            # Identify rows with differences
            # check for rows that are in old df but not in new df
            # based on primary key
            file.write("## Deleted\n\n")
            deleted_rows = old_df[~old_df.index.isin(new_df.index)]
            if not deleted_rows.empty:
                file.write(f"{deleted_rows.to_csv()}\n\n")

            file.write("## Added\n\n")
            added_rows = new_df[~new_df.index.isin(old_df.index)]
            if not added_rows.empty:
                file.write(f"{added_rows.to_csv()}\n\n")

            # Must sort index in order to compare dataframes cell-wise
            shared_rows_old = (
                old_df[old_df.index.isin(new_df.index)].sort_index().sort_index(axis=1)
            )
            shared_rows_new = (
                new_df[new_df.index.isin(old_df.index)].sort_index().sort_index(axis=1)
            )
            old_types = shared_rows_old.map(type_or_na)
            new_types = shared_rows_new.map(type_or_na)
            different_types = old_types != new_types
            if different_types.any().any():
                row, col = list(zip(*different_types.values.nonzero()))[0]
                print(f"Type mismatch in {table_name} at ({row}, {col})")
                print(f"Old type: {old_types.iat[row, col]}")
                print(f"New type: {new_types.iat[row, col]}")
                print(f"Old value: {shared_rows_old.iat[row, col]}")
                print(f"New value: {shared_rows_new.iat[row, col]}")
                raise TypeError("Type mismatch")
            unequal_mask = ~(
                (shared_rows_old == shared_rows_new)
                | (shared_rows_old.isna() & shared_rows_new.isna())
            )

            file.write("## Changed\n\n")
            changed_rows = shared_rows_new[unequal_mask.any(axis=1)].copy()
            changed_rows["columns_changed"] = unequal_mask.apply(
                lambda row: shared_rows_new.columns[row].tolist(), axis=1
            )
            if not changed_rows.empty:
                file.write(f"{changed_rows.to_csv()}\n\n")

            diff_dict[table_name] = {
                "deleted_rows": deleted_rows,
                "added_rows": added_rows,
                "changed_rows": changed_rows,
            }

        print("✔")

    return diff_dict

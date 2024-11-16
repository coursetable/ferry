# TODO load data into pandas

import pandas as pd
import csv
import json
import ujson
import ast
from io import StringIO
import logging
from pathlib import Path

from sqlalchemy import MetaData, text, inspect

from ferry.crawler.classes.parse import normalize_unicode
from ferry.database import Database, Base
from ferry.transform import transform


queries_dir = Path(__file__).parent / "queries"

# TODO - need primary key for course_meetings

primary_keys = {
    "flags": ["flag_id"],
    "course_flags": ["course_id"],
    "professors": ["professor_id"],
    "course_professors": ["course_id"],
    "courses": ["course_id"],
    "listings": ["listing_id"],
    "buildings": ["code"],
    "locations": ["location_id"],
    "course_meetings": ["course_id"],
}

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
        "all": ["time_added", "last_updated"],  # ignore the timestamps
        "flags": [],
        "course_flags": [],
        "professors": [],
        "course_professors": [],
        'courses': ['same_course_and_profs_id', 'same_course_id', 'same_prof_id', 'last_offered_course_id'],
        "listings": [],
        "buildings": [],
        "locations": [],
        "course_meetings": []
    }

    for col_name in row.index.tolist():
        if ("_old" not in col_name):
            continue
        col_name = col_name.replace("_old", "")

        if (col_name in cols_to_exclude[table_name] or col_name in cols_to_exclude["all"]):
            continue

        old_value = row[col_name + "_old"]
        new_value = row[col_name + "_new"]

        if isinstance(old_value, list) or isinstance(new_value, list):
            old_value = ujson.loads(
                str(old_value).replace("'", '"'))  # fix quotes
            new_value = ujson.loads(str(new_value).replace("'", '"'))
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
                    # todo - maybe a better condition here
                    if (abs(old_value - new_value) < 0.000001):
                        return False
                else:

                    old_value = str(old_value).replace(
                        '"', "'").replace('\\', '').strip("'")
                    new_value = str(new_value).replace(
                        '"', "'").replace('\\', '').strip("'")
                    
                    try:
                        old_value = json.loads(old_value)
                        new_value = json.loads(new_value)
                    except:
                        pass
                if old_value != new_value:
                    print(
                        f"column: {col_name}, old: {old_value}, new: {new_value}")
                    return True
            elif (pd.isna(old_value) and not pd.isna(new_value)) or (not pd.isna(old_value) and pd.isna(new_value)):
                # deleted or added a specific value
                print(
                        f"column: {col_name}, old: {old_value}, new: {new_value}")
                return True
    return False


def generate_diff(tables_old: dict[str, pd.DataFrame],
                  tables_new: dict[str, pd.DataFrame], output_dir: str):

    diff_dict = {}

    for table_name in primary_keys.keys():
        if table_name not in tables_new.keys() or table_name not in tables_old.keys():
            raise ValueError(f"Table {table_name} not found in new tables")

        print(f"Computing diff for table {table_name} ...", end=" ")

        output_file_path = Path(output_dir) / (table_name + ".md")

        with open(output_file_path, "w+") as file:
            # check difference between old df and new df and output to above file path
            old_df = tables_old[table_name]
            new_df = tables_new[table_name]

            # TODO - better way to do this?
            pk = primary_keys[table_name][0]

            # Identify rows with differences

            # check for rows that are in old df but not in new df
            # based on primary key
            file.write("## Deleted rows in new table: \n")

            deleted_rows = old_df[~old_df[pk].isin(new_df[pk])]
            if not deleted_rows.empty:
                file.write(f"{deleted_rows.to_csv()}\n")

            file.write("## Added rows in new table: \n")
            # check for rows that have been added
            added_rows = new_df[~new_df[pk].isin(old_df[pk])]
            if not added_rows.empty:
                file.write(f"{added_rows.to_csv()}\n")

            if table_name == "course_flags":
                old_df = old_df.groupby("course_id")[
                    "flag_id"].apply(frozenset)
                new_df = new_df.groupby("course_id")[
                    "flag_id"].apply(frozenset)
            elif table_name == "course_professors":
                old_df = old_df.groupby("course_id")[
                    "professor_id"].apply(frozenset)
                new_df = new_df.groupby("course_id")[
                    "professor_id"].apply(frozenset)

            merged_df = pd.merge(old_df, new_df, on=pk,
                                 how="inner", suffixes=('_old', '_new'))

            changed_rows = merged_df[merged_df.apply(
                check_change, args=(table_name,), axis=1)]

            file.write("## Changed rows in new table: \n")

            if not changed_rows.empty:
                file.write(f"{changed_rows.to_csv()}\n")

            diff_dict[table_name] = {
                "deleted_rows": deleted_rows,
                "added_rows": added_rows,
                "changed_rows": changed_rows
            }

        print("✔")

    return diff_dict


if __name__ == "__main__":
    tables_old = get_dfs("postgresql://postgres:postgres@db:5432/postgres")
    tables_new = transform(data_dir=Path("/workspaces/ferry/data"))
    generate_diff(tables_old, tables_new, "/workspaces/ferry/diff")

import csv
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import MetaData

from ferry.database import Database, Base


class DatabaseStagingError(Exception):
    """
    Object for import exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


def copy_from_stringio(conn, table: pd.DataFrame, table_name: str):
    """
    Save DataFrame in-memory and migrate to database with copy_from().

    Parameters
    ----------
    conn:
        psycopg2 connection object.
    table:
        DataFrame to import.
    table_name:
        name of target table.

    Returns
    -------
    """
    # create in-memory buffer for DataFrame
    buffer = StringIO()

    table.to_csv(
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
            buffer, table_name, columns=table.columns, sep="\t", null="NULL"
        )
    except Exception as error:
        conn.rollback()
        cursor.close()
        raise DatabaseStagingError from error

    # print(f"Successfully copied {table_name}")
    cursor.close()


def stage(data_dir: Path, database: Database):
    """
    Load transformed CSVs into staged database tables.
    """

    print("\nReading in tables from CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # common pd.read_csv arguments
    general_csv_kwargs: dict[Any, Any] = {"index_col": 0, "low_memory": False}

    def load_csv(
        table_name: str, csv_kwargs: dict[str, Any] | None = None
    ) -> pd.DataFrame:
        """
        Loads a CSV given a table name.

        Parameters
        ----------
        table_name:
            name of table to load
        csv_kwargs:
            additional arguments to pass to pandas.read_csv
        """

        if csv_kwargs is None:
            csv_kwargs = {}

        merged_kwargs = general_csv_kwargs.copy()
        merged_kwargs.update(csv_kwargs)

        return pd.read_csv(csv_dir / f"{table_name}.csv", **merged_kwargs)

    seasons = load_csv("seasons")

    courses = load_csv(
        "courses",
        {
            "dtype": {
                "average_rating_n": "Int64",
                "average_workload_n": "Int64",
                "average_rating_same_professors_n": "Int64",
                "average_workload_same_professors_n": "Int64",
                "last_offered_course_id": "Int64",
                "last_enrollment_course_id": "Int64",
                "last_enrollment": "Int64",
                "last_enrollment_season_code": "Int64",
            }
        },
    )
    listings = load_csv("listings", {"dtype": {"section": str}})
    professors = load_csv(
        "professors",
        {
            "dtype": {
                "average_rating_n": "Int64",
            }
        },
    )
    course_professors = load_csv(
        "course_professors",
        {
            "dtype": {
                "professor_id": "Int64",
                "course_id": "Int64",
            }
        },
    )
    flags = load_csv("flags")
    course_flags = load_csv("course_flags")

    evaluation_questions = load_csv("evaluation_questions")
    evaluation_narratives = load_csv("evaluation_narratives")
    evaluation_ratings = load_csv("evaluation_ratings")
    evaluation_statistics = load_csv(
        "evaluation_statistics",
        {
            "dtype": {
                "enrolled": "Int64",
                "responses": "Int64",
                "declined": "Int64",
                "no_response": "Int64",
            }
        },
    )

    # Define mapping of tables to their respective DataFrames
    tables = {
        "seasons_staged": seasons,
        "courses_staged": courses,
        "listings_staged": listings,
        "professors_staged": professors,
        "course_professors_staged": course_professors,
        "flags_staged": flags,
        "course_flags_staged": course_flags,
        "evaluation_questions_staged": evaluation_questions,
        "evaluation_narratives_staged": evaluation_narratives,
        "evaluation_ratings_staged": evaluation_ratings,
        "evaluation_statistics_staged": evaluation_statistics,
    }

    print("\033[F", end="")
    print(f"Reading in tables from CSVs... ✔")

    # --------------------------
    # Replace tables in database
    # --------------------------

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=database.Engine)

    # Drop all tables
    print("\nDropping all tables...")
    db_meta.drop_all(
        bind=database.Engine,
        tables=list(reversed(db_meta.sorted_tables)),
        checkfirst=True,
    )
    print("\033[F", end="")
    print("Dropping all tables... ✔")

    # Add new staging tables
    print("\nAdding new staging tables...")
    connection = database.Engine.raw_connection()
    Base.metadata.create_all(database.Engine)
    for table in Base.metadata.sorted_tables:
        if table.name in tables:
            copy_from_stringio(connection, tables[table.name], f"{table.name}")
        else:
            raise ValueError(
                f"{table.name} defined in Base metadata, but there is no data for it."
            )
    connection.commit()

    print("\033[F", end="")
    print("Adding new staging tables... ✔")

    # Print all added tables
    print("\n[Table Summary]")
    for table in Base.metadata.sorted_tables:
        print(f"{table.name}")

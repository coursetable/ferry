"""
Load transformed CSVs into staged database tables.

Used immediately after transform.py and immediately before deploy.py.
"""
from typing import Any, Dict

import pandas as pd
from sqlalchemy import MetaData

from ferry import config, database
from ferry.database.models import Base
from ferry.includes.staging import copy_from_stringio

if __name__ == "__main__":

    print("\n[Reading in tables from CSVs]")

    csv_dir = config.DATA_DIR / "importer_dumps"

    # common pd.read_csv arguments
    general_csv_kwargs: Dict[Any, Any] = {"index_col": 0, "low_memory": False}

    def load_csv(table_name: str, csv_kwargs: Dict[str, Any] = None) -> pd.DataFrame:
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
    course_professors = load_csv("course_professors")
    flags = load_csv("flags")
    course_flags = load_csv("course_flags")

    discussions = load_csv(
        "discussions", {"dtype": {"section_crn": "Int64", "section": str}}
    )
    course_discussions = load_csv("course_discussions")

    demand_statistics = load_csv("demand_statistics")

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

    fasttext_similars = load_csv(
        "fasttext_similars", {"dtype": {"source": "Int64", "target": "Int64"}}
    )
    tfidf_similars = load_csv(
        "tfidf_similars", {"dtype": {"source": "Int64", "target": "Int64"}}
    )

    # --------------------------
    # Replace tables in database
    # --------------------------
    print("\n[Clearing staging tables]")

    # ordered tables defined only in our model
    alchemy_tables = database.Base.metadata.sorted_tables

    # sorted tables in the database
    db_meta = MetaData(bind=database.Engine)
    db_meta.reflect()

    # drop old staging tables
    conn = database.Engine.connect()
    delete = conn.begin()
    # loop in reverse sorted order to handle dependencies
    for table in db_meta.sorted_tables[::-1]:
        if table.name.endswith("_staged"):
            print(f"Dropping table {table.name}")
            conn.execute(f"DROP TABLE IF EXISTS {table.name} CASCADE;")
    delete.commit()

    Base.metadata.create_all(database.Engine)

    # -------------
    # Update tables
    # -------------

    print("\n[Staging new tables]")

    raw_conn = database.Engine.raw_connection()

    # seasons
    copy_from_stringio(raw_conn, seasons, "seasons_staged")

    # courses
    copy_from_stringio(raw_conn, courses, "courses_staged")
    copy_from_stringio(raw_conn, listings, "listings_staged")
    copy_from_stringio(raw_conn, professors, "professors_staged")
    copy_from_stringio(raw_conn, course_professors, "course_professors_staged")
    copy_from_stringio(raw_conn, flags, "flags_staged")
    copy_from_stringio(raw_conn, course_flags, "course_flags_staged")

    # discussion sections
    copy_from_stringio(raw_conn, discussions, "discussions_staged")
    copy_from_stringio(raw_conn, course_discussions, "course_discussions_staged")

    # demand statistics
    copy_from_stringio(raw_conn, demand_statistics, "demand_statistics_staged")

    # evaluations
    copy_from_stringio(raw_conn, evaluation_questions, "evaluation_questions_staged")
    copy_from_stringio(raw_conn, evaluation_narratives, "evaluation_narratives_staged")
    copy_from_stringio(raw_conn, evaluation_ratings, "evaluation_ratings_staged")
    copy_from_stringio(raw_conn, evaluation_statistics, "evaluation_statistics_staged")

    # similar courses
    copy_from_stringio(raw_conn, fasttext_similars, "fasttext_similars_staged")
    copy_from_stringio(raw_conn, tfidf_similars, "tfidf_similars_staged")

    raw_conn.commit()

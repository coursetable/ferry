import pandas as pd
import ujson
from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql.schema import ForeignKeyConstraint

from ferry import config, database
from ferry.config import DATABASE_CONNECT_STRING
from ferry.includes.importer import copy_from_stringio
from ferry.includes.tqdm import tqdm

"""
===============================================================
This script loads transformed CSVs into staged database tables.
===============================================================
"""

if __name__ == "__main__":

    print("\n[Reading in tables from CSVs]")

    csv_dir = config.DATA_DIR / "importer_dumps"

    seasons = pd.read_csv(csv_dir / "seasons.csv", index_col=0)

    courses = pd.read_csv(csv_dir / "courses.csv", index_col=0)
    listings = pd.read_csv(csv_dir / "listings.csv", index_col=0)
    professors = pd.read_csv(csv_dir / "professors.csv", index_col=0)
    course_professors = pd.read_csv(csv_dir / "course_professors.csv", index_col=0)

    demand_statistics = pd.read_csv(csv_dir / "demand_statistics.csv", index_col=0)

    evaluation_questions = pd.read_csv(
        csv_dir / "evaluation_questions.csv", index_col=0
    )
    evaluation_narratives = pd.read_csv(
        csv_dir / "evaluation_narratives.csv", index_col=0
    )
    evaluation_ratings = pd.read_csv(csv_dir / "evaluation_ratings.csv", index_col=0)
    evaluation_statistics = pd.read_csv(
        csv_dir / "evaluation_statistics.csv", index_col=0
    )

    # fix datatype assumptions by pandas
    listings["section"] = listings["section"].astype(str)
    evaluation_statistics["enrolled"] = evaluation_statistics["enrolled"].astype(
        "Int64"
    )
    evaluation_statistics["responses"] = evaluation_statistics["responses"].astype(
        "Int64"
    )
    evaluation_statistics["declined"] = evaluation_statistics["declined"].astype(
        "Int64"
    )
    evaluation_statistics["no_response"] = evaluation_statistics["no_response"].astype(
        "Int64"
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
            conn.execute(f'ALTER TABLE IF EXISTS "{table}" DISABLE TRIGGER ALL;')
            conn.execute(table.delete())
            conn.execute(f'ALTER TABLE IF EXISTS "{table}" ENABLE TRIGGER ALL;')
    delete.commit()

    staging_tables = []

    # create staging tables based on SQLAlchemy models
    for table in alchemy_tables:

        print(f"Creating table {table.name}_staged")

        args = []
        for column in table.columns:
            args.append(column.copy())

        for constraint in table.constraints:
            if not isinstance(constraint, ForeignKeyConstraint):
                args.append(constraint.copy())

        staging_tables.append(
            Table(
                f"{table.name}_staged",
                table.metadata,
                extend_existing=True,
                *args,
            )
        )

    db_meta.create_all(tables=staging_tables)

    print("\n[Staging new tables]")

    raw_conn = database.Engine.raw_connection()

    # seasons
    copy_from_stringio(raw_conn, seasons, "seasons_staged")

    # courses
    copy_from_stringio(raw_conn, courses, "courses_staged")
    copy_from_stringio(raw_conn, listings, "listings_staged")
    copy_from_stringio(raw_conn, professors, "professors_staged")
    copy_from_stringio(raw_conn, course_professors, "course_professors_staged")

    # demand statistics
    copy_from_stringio(raw_conn, demand_statistics, "demand_statistics_staged")

    # evaluations
    copy_from_stringio(raw_conn, evaluation_questions, "evaluation_questions_staged")
    copy_from_stringio(raw_conn, evaluation_narratives, "evaluation_narratives_staged")
    copy_from_stringio(raw_conn, evaluation_ratings, "evaluation_ratings_staged")
    copy_from_stringio(raw_conn, evaluation_statistics, "evaluation_statistics_staged")

    raw_conn.commit()

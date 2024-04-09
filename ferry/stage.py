import csv
from io import StringIO
from pathlib import Path
from typing import Any
from contextlib import contextmanager

import pandas as pd
from sqlalchemy import MetaData
import sqlalchemy
from typing import Tuple, Dict

from ferry.database import Database
from ferry.database.models import Base, Listing
from ferry.includes.staging import copy_from_stringio

@contextmanager
def session_scope(Session):
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def fetch_existing_listings(engine):
    """Fetch existing listings from the database."""
    with engine.connect() as connection:
        existing_listings_df = pd.read_sql_table('listings', con=connection)
    return existing_listings_df

def compare_listings(new_listings: pd.DataFrame, existing_listings: pd.DataFrame) ->  Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compare new and existing course records to identify new, updated, and deleted courses.

    Returns a tuple of DataFrames for new, updated, and deleted courses.
    """
    print(new_listings.columns)
    print(existing_listings.columns)
    # Merge new and existing listings on crn and season_code
    merged_listings = new_listings.merge(
        existing_listings,
        how="outer",
        on=["crn", "season_code"],
        suffixes=("_new", "_existing"),
    )

    # New listings are those present in the new data but not in existing records
    new_listings = merged_listings[merged_listings["course_id_existing"].isnull()]

    # Updated listings are those present in both new and existing records but with different values (do we want)
    # TODO
    # Deleted listings are those present in existing records but not in the new data
    deleted_listings = merged_listings[merged_listings["course_id_new"].isnull()]

    return new_listings, deleted_listings

def insert_new_listings(db: Database, new_listings_df: pd.DataFrame) -> None:
    """Insert new listings into the database."""
    # Fetch existing listings to prevent duplicate entries
    # may need to run SELECT setval('listings_staged_listing_id_seq', (SELECT MAX(listing_id) FROM listings_staged) + 1);
    # or SELECT setval('listings_staged_listing_id_seq', COALESCE((SELECT MAX(listing_id) FROM listings), 1));
    existing_listings_df = fetch_existing_listings(db.Engine)
    existing_crns = set(existing_listings_df['crn'])

    # Filter out existing CRNs from new listings
    new_listings_df = new_listings_df[~new_listings_df['crn'].isin(existing_crns)]

    if not new_listings_df.empty:
        # Drop 'listing_id' column to let the database handle it through auto-increment
        new_listings_df.drop(columns=['listing_id'], errors='ignore', inplace=True)
        
        # Insert new listings into the database
        new_listings_df.to_sql('listings', con=db.Engine, if_exists='append', index=False)
        print(f"Inserted {len(new_listings_df)} new listings.")


def stage_listings(data_dir: Path, db: Database):
    """
    Stages new listings, updates existing ones, and handles deleted listings.
    """
    print("\nReading in tables from CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # common pd.read_csv arguments
    general_csv_kwargs: dict[Any, Any] = {"index_col": 0, "low_memory": False}

    def load_csv(table_name: str, csv_kwargs: dict[str, Any] = None) -> pd.DataFrame:
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
    
    # Load new listings data from CSV
    new_listings_df = load_csv("listings", {"dtype": {"section": str}})
    print(new_listings_df[['course_id']].head())

    # Confirm there are no null values in 'course_id' right before insertion
    print(new_listings_df['course_id'].isnull().any())

    # Confirm the data types
    print(new_listings_df.dtypes)
    # Insert new listings into the database
    insert_new_listings(db, new_listings_df)
    print("Done!")

    # Fetch existing listings from the database
    with session_scope(db.Session) as session:
        existing_listings_df = fetch_existing_listings(session)

        # Compare new and existing listings
        new_listings, deleted_listings = compare_listings(
            new_listings_df, existing_listings_df
        )

        # Print summary
        print("\n[Listings Summary]")
        print(f"New listings: {len(new_listings)}")
        print(f"Deleted listings: {len(deleted_listings)}")
        if new_listings_df['course_id'].isnull().any():
            raise ValueError("Null 'course_id' values found in listings DataFrame.")



        # --------------------------
        # Update existing listings when we link w course
        # --------------------------

        """if not updated_listings.empty:
            print("\nUpdating existing listings...")

            # Update existing listings
            for _, row in updated_listings.iterrows():
                session.query(Listing).filter(
                    Listing.crn == row["crn"],
                    Listing.season_code == row["season_code"],
                ).update(
                    {
                        "course_id": row["course_id_new"],
                        "professor_id": row["professor_id_new"],
                        "section": row["section_new"],
                        "year": row["year_new"],
                        "enrollment": row["enrollment_new"],
                        "average_rating": row["average_rating_new"],
                        "average_workload": row["average_workload_new"],
                        "average_rating_same_professors": row["average_rating_same_professors_new"],
                        "average_workload_same_professors": row["average_workload_same_professors_new"],
                        "flags": row["flags_new"],
                        "course_flags": row["course_flags_new"],
                        "discussions": row["discussions_new"],
                        "course_discussions": row["course_discussions_new"],
                    }
                )

            print("\033[F", end="")
            print("Updating existing listings... ✔") """
            
        # --------------------------

        # --------------------------
        # Insert new listings
        # --------------------------

        required_columns = ['listing_id', 'school', 'subject', 'number', 'course_code', 'section', 'season_code', 'crn']
        actual_columns = [col for col in required_columns if col in new_listings.columns]
        # this is pretty jank def a better way but was getting key errors
        new_listings_adjusted = new_listings[actual_columns]

        if not new_listings.empty:
            print("\nInserting new listings...")

            # Insert new listings
            new_listings_adjusted.to_sql(
                "listings_staged",
                con=db.Engine,
                if_exists="append",
                index=False,
            )

            print("\033[F", end="")
            print("Inserting new listings... ✔")

        # --------------------------

        # --------------------------
        # Delete listings
        # --------------------------

        if not deleted_listings.empty:
            print("\nDeleting listings...")

            # Delete listings
            for _, row in deleted_listings.iterrows():
                session.query(Listing).filter(
                    Listing.crn == row["crn"],
                    Listing.season_code == row["season_code"],
                ).delete()

            print("\033[F", end="")
            print("Deleting listings... ✔")

        # --------------------------

        # Commit changes
        session.commit()


    


def stage(data_dir: Path, database: Database):
    """
    Load transformed CSVs into staged database tables.
    """

    print("\nReading in tables from CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    # common pd.read_csv arguments
    general_csv_kwargs: dict[Any, Any] = {"index_col": 0, "low_memory": False}

    def load_csv(table_name: str, csv_kwargs: dict[str, Any] = None) -> pd.DataFrame:
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
        }
    )
    flags = load_csv("flags")
    course_flags = load_csv("course_flags")

    # discussions = load_csv(
    #     "discussions", {"dtype": {"section_crn": "Int64", "section": str}}
    # )
    # course_discussions = load_csv("course_discussions")

    # demand_statistics = load_csv("demand_statistics")

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
        # "discussions_staged": discussions,
        # "course_discussions_staged": course_discussions,
        # "demand_statistics_staged": demand_statistics,
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
        tables=reversed(db_meta.sorted_tables),
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
            raise ValueError(f"{table.name} defined in Base metadata, but there is no data for it.")
    connection.commit()
        
    print("\033[F", end="")
    print("Adding new staging tables... ✔")

    # Print all added tables
    print("\n[Table Summary]")
    for table in Base.metadata.sorted_tables:
        print(f"{table.name}")
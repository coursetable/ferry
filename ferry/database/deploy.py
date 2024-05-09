import logging
from pathlib import Path

import sqlalchemy
from sqlalchemy import MetaData, text

from ferry import database

resource_dir = Path(__file__).parent.parent / "resources"


def listing_invariants(session: sqlalchemy.orm.session.Session):
    """
    Check listing invariants.

    Check invariant:
        listing.season_code == course.season_code if listing.course_id == course.course_id.
    """

    for (
        listing_id,
        course_id,
        listing_season_code,
        course_season_code,
    ) in session.query(
        database.Listing.listing_id,
        database.Listing.course_id,
        database.Listing.season_code,
        database.Course.season_code,
    ).filter(
        database.Listing.course_id == database.Course.course_id
    ):
        if listing_season_code != course_season_code:
            raise database.InvariantError(
                f"listing {listing_id} has mismatched season_code with course {course_id}"
            )


def question_invariants(session: sqlalchemy.orm.session.Session):
    """
    Check question invariants.

    Check invariant:
        evaluation_questions.options is null iff evaluation_questions.is_narrative = True
    """

    for question in session.query(database.EvaluationQuestion):
        narrative = question.is_narrative
        options = bool(question.options)
        if narrative and options:
            raise database.InvariantError(f"narrative question {question} has options")
        if not narrative and not options:
            raise database.InvariantError(f"ratings question {question} lacks options")


def course_invariants(session: sqlalchemy.orm.session.Session):
    """
    Check course invariants.

    Check invariant:
        every course should have at least one listing.
    """

    courses_no_listings = (
        session.query(database.Listing)
        .filter(
            ~database.Listing.course_id.in_(session.query(database.Course.course_id))
        )
        .all()
    )

    if courses_no_listings:

        no_listing_courses = [str(course) for course in courses_no_listings]

        raise database.InvariantError(
            f"the following courses have no listings: {', '.join(no_listing_courses)}"
        )


def search_setup(session: sqlalchemy.orm.session.Session):
    """
    Set up an aggregated course information table.

    Used by CourseTable to pull JSONs for client-side catalog browsing.
    """
    logging.debug("Creating tmp table")
    with open(f"{resource_dir}/computed_listing_info_tmp.sql") as tmp_file:
        tmp_sql = tmp_file.read()
        session.execute(text(tmp_sql))

    logging.debug("Setting columns to not null if possible")
    table_name = "computed_listing_info_tmp"
    for _, _, column_name in session.execute(
        text(
            # Get the list of columns in the table.
            f"""
        SELECT table_schema, table_name, column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}';
        """
        )
    ):
        (null_count,) = session.execute(
            text(
                f"""
            SELECT count(*) FROM {table_name} WHERE {column_name} IS NULL ;
            """
            )
        ).first()
        if null_count == 0:
            session.execute(
                text(
                    f"""
                ALTER TABLE {table_name} ALTER COLUMN {column_name} SET NOT NULL ;
                """
                )
            )
            logging.debug(f"  {column_name} not null")

    logging.debug("Swapping in the table")
    with open(f"{resource_dir}/computed_listing_info_swap.sql") as swap_file:
        swap_sql = swap_file.read()
        session.execute(text(swap_sql))


def deploy(db: database.Database):
    """
    Deploy staged tables to main ones and regenerate database.

    - Checks the database invariants on staged tables.
    - If invariants pass, promotes staged tables to actual ones,
        updates indexes, and recomputes search views
    - If any invariant fails, exits with no changes to tables.
    """

    # ------------------------------------
    # Specify invariant checking functions
    # ------------------------------------

    all_items = [
        listing_invariants,
        course_invariants,
        question_invariants,
    ]

    # --------------------------------------
    # Check if all staged tables are present
    # --------------------------------------

    # sorted tables in the database
    db_meta = MetaData()
    db_meta.reflect(bind=db.Engine)

    # ordered tables defined only in our model
    alchemy_tables = database.Base.metadata.sorted_tables
    target_tables = [table.name[:-7] for table in alchemy_tables]

    db_tables = {x.name for x in db_meta.sorted_tables}

    if any(table.name not in db_tables for table in alchemy_tables):
        raise database.MissingTablesError(
            "Not all staged tables are present. Run stage.py again?"
        )

    # ------------------------------
    # Check invariants on new tables
    # ------------------------------

    print("\nChecking table invariants...")

    items = all_items

    for fn in items:
        if fn.__doc__:
            logging.debug(f"{fn.__doc__.strip()}")
        else:
            logging.debug(f"Running: {fn.__name__}")

        with database.session_scope(db.Session) as db_session:
            fn(db_session)

    print("\033[F", end="")
    print("Checking table invariants... ✔")

    # -------------------------------------
    # Upgrade staged tables to primary ones
    # -------------------------------------

    print("\nReplacing old tables with staged...")

    conn = db.Engine.connect()

    # keep track of main table constraints and indexes
    # because staged tables do not have foreign key relationships

    replace = conn.begin()
    conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))

    # drop and update tables in reverse dependency order
    for table in target_tables:

        logging.debug(f"Updating table {table}")

        # remove the old table if it is present before
        conn.execute(text(f"DROP TABLE IF EXISTS {table}_old CASCADE;"))
        # rename current main table to _old
        # (keep the old tables instead of dropping them
        # so we can rollback if invariants don't pass)
        conn.execute(text(f'ALTER TABLE IF EXISTS "{table}" RENAME TO {table}_old;'))
        # rename staged table to main
        conn.execute(text(f'ALTER TABLE IF EXISTS "{table}_staged" RENAME TO {table};'))

    replace.commit()

    print("\033[F", end="")
    print("Replacing old tables with staged... ✔")

    # -----------------
    # Remove old tables
    # -----------------

    print("\nDeleting temporary old tables...")
    delete = conn.begin()
    conn.execute(text("SET CONSTRAINTS ALL DEFERRED;"))
    for table in target_tables:
        logging.debug(f"Dropping table {table}_old")
        conn.execute(text(f"DROP TABLE IF EXISTS {table}_old CASCADE;"))

    delete.commit()
    print("\033[F", end="")
    print("Deleting temporary old tables... ✔")

    # ----------------------
    # Rename _staged indexes
    # ----------------------

    print("\nRenaming indexes...")

    # sorted tables in the database
    db_meta.reflect(bind=db.Engine)

    # delete previous staged indexes
    delete_indexes = conn.begin()
    for meta_table in db_meta.sorted_tables:
        for index in meta_table.indexes:
            # remove the staged indexes
            if index.name and "_staged" in index.name:
                # conn.execute(text(schema.DropIndex(index)))
                renamed = index.name.replace("_staged", "")
                conn.execute(
                    text(f"ALTER INDEX IF EXISTS {index.name} RENAME TO {renamed};")
                )
        # primary key indexes are not listed under meta_table.indexes
        # so just rename these if they exist
        conn.execute(
            text(
                f"ALTER INDEX IF EXISTS pk_{meta_table.name}_staged RENAME TO pk_{meta_table.name};"
            )
        )
    delete_indexes.commit()

    print("\033[F", end="")
    print("Renaming indexes... ✔")

    # -------
    # Reindex
    # -------

    print("\nReindexing...")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute(
        text("REINDEX DATABASE postgres;")
    )
    print("\033[F", end="")
    print("Reindexing... ✔")

    # -----------------------------------
    # Set up materialized view and search
    # -----------------------------------

    # generate computed tables and full-text-search
    print("\nSetting up computed tables...")
    with database.session_scope(db.Session) as db_session:
        search_setup(db_session)

    print("\033[F", end="")
    print("Setting up computed tables... ✔")

    # -------------
    # Final summary
    # -------------

    # Print row counts for each table.
    print("\n[Table Statistics]")
    with database.session_scope(db.Session) as db_session:
        with open(f"{resource_dir}/table_sizes.sql") as file:
            SUMMARY_SQL = file.read()

        result = db_session.execute(text(SUMMARY_SQL))
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

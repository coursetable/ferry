"""
Deploy staged tables to main ones and regenerate database.

- Checks the database invariants on staged tables.
- If invariants pass, promotes staged tables to actual ones,
  updates indexes, and recomputes search views
- If any invariant fails, exits with no changes to tables.

"""

import argparse

import sqlalchemy
from sqlalchemy import MetaData

from ferry import config, database
from ferry.includes.tqdm import tqdm


def listing_invariants(session):
    """
    Check invariant:
        listing.season_code == course.season_code if listing.course_id == course.course_id.
    """
    for listing_id, course_id, listing_season_code, course_season_code in session.query(
        database.Listing.listing_id,
        database.Listing.course_id,
        database.Listing.season_code,
        database.Course.season_code,
    ).filter(database.Listing.course_id == database.Course.course_id):
        if listing_season_code != course_season_code:
            raise database.InvariantError(
                f"listing {listing_id} has mismatched season_code with course {course_id}"
            )


def question_invariants(session):
    """
    Check invariant:
        evaluation_questions.options is null iff evaluation_questions.is_narrative = True
    """
    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        narrative = question.is_narrative
        options = bool(question.options)
        if narrative and options:
            raise database.InvariantError(f"narrative question {question} has options")
        if not narrative and not options:
            raise database.InvariantError(f"ratings question {question} lacks options")


def question_tag_invariant(session):
    """
    Check invariant:
        all questions sharing a tag also share is_narrative and len(options)
    """
    # Dictionary of question_code -> (is_narrative, len(options))
    tag_cache = {}

    def optlen(options):
        return len(options) if options else -1

    for question in session.query(
        database.EvaluationQuestion
    ):  # type: database.EvaluationQuestion
        if not question.tag:
            continue

        if question.tag not in tag_cache:
            tag_cache[question.tag] = (question.is_narrative, optlen(question.options))
        else:
            narrative, count = tag_cache[question.tag]
            if question.is_narrative != narrative or count != optlen(question.options):
                raise database.InvariantError(f"mismatched tag {question.tag}")


def course_invariants(session):
    """
    Check invariant:
        every course should have at least one listing.
    """
    courses_no_listings = (
        session.query(database.Course)
        .select_from(database.Listing)
        .join(database.Listing.course, isouter=True)
        .group_by(database.Course.course_id)
        .having(sqlalchemy.func.count(database.Listing.listing_id) == 0)
    ).all()

    if courses_no_listings:

        no_listing_courses = [str(course) for course in courses_no_listings]

        raise database.InvariantError(
            f"the following courses have no listings: {', '.join(no_listing_courses)}"
        )


def search_setup(session):
    """
    Set up materialized view and search function
    """

    with open(f"{config.RESOURCE_DIR}/search.sql") as file:
        sql = file.read()
    session.execute(sql)


if __name__ == "__main__":

    # ------------------------------------
    # Specify invariant checking functions
    # ------------------------------------

    all_items = [
        listing_invariants,
        course_invariants,
        question_invariants,
        question_tag_invariant,
    ]

    def _match(name: str):
        """
        Get a function object by name (string)
        """
        for checking_function in all_items:
            if checking_function.__name__ == name:
                return checking_function
        raise ValueError(f"cannot find item with name {name}")

    parser = argparse.ArgumentParser(
        description="Generate computed fields and check invariants"
    )
    parser.add_argument(
        "--items",
        nargs="+",
        help="which items to run",
        default=None,
        required=False,
    )

    # --------------------------------------
    # Check if all staged tables are present
    # --------------------------------------

    # sorted tables in the database
    db_meta = MetaData(bind=database.Engine)
    db_meta.reflect()

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

    # check invariants
    print("\n[Checking table invariants]")

    # check invariants
    args = parser.parse_args()
    if args.items:
        items = [_match(name) for name in args.items]
    else:
        items = all_items

    for fn in items:
        if fn.__doc__:
            tqdm.write(f"{fn.__doc__.strip()}")
        else:
            tqdm.write(f"Running: {fn.__name__}")

        with database.session_scope(database.Session) as db_session:
            fn(db_session)

    print("All invariants passed")

    # -------------------------------------
    # Upgrade staged tables to primary ones
    # -------------------------------------

    print("\n[Replacing old tables with staged]")

    conn = database.Engine.connect()

    # keep track of main table constraints and indexes
    # because staged tables do not have foreign key relationships

    replace = conn.begin()
    conn.execute("SET CONSTRAINTS ALL DEFERRED;")

    # drop and update tables in reverse dependnecy order
    for table in target_tables:

        print(f"Updating table {table}")

        # remove the old table if it is present before
        conn.execute(f"DROP TABLE IF EXISTS {table}_old CASCADE;")
        # rename current main table to _old
        # (keep the old tables instead of dropping them
        # so we can rollback if invariants don't pass)
        conn.execute(f'ALTER TABLE IF EXISTS "{table}" RENAME TO {table}_old;')
        # rename staged table to main
        conn.execute(f'ALTER TABLE IF EXISTS "{table}_staged" RENAME TO {table};')

    replace.commit()

    # -----------------
    # Remove old tables
    # -----------------

    print("\n[Deleting temporary old tables]")
    delete = conn.begin()
    conn.execute("SET CONSTRAINTS ALL DEFERRED;")
    for table in target_tables:
        print(f"Dropping table {table}_old")
        conn.execute(f"DROP TABLE IF EXISTS {table}_old CASCADE;")

    delete.commit()

    # ----------------------
    # Rename _staged indexes
    # ----------------------

    print("\n[Renaming indexes]")

    # sorted tables in the database
    db_meta.reflect()

    # delete previous staged indexes
    delete_indexes = conn.begin()
    for table in db_meta.sorted_tables:
        for index in table.indexes:
            # remove the staged indexes
            if "_staged" in index.name:
                # conn.execute(schema.DropIndex(index))
                renamed = index.name.replace("_staged", "")
                conn.execute(f"ALTER INDEX IF EXISTS {index.name} RENAME TO {renamed};")
        # primary key indexes are not listed under table.indexes
        # so just rename these if they exist
        conn.execute(
            f"ALTER INDEX IF EXISTS pk_{table.name}_staged RENAME TO pk_{table.name};"
        )
    delete_indexes.commit()

    # -------
    # Reindex
    # -------

    print("\n[Reindexing]")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute(
        "REINDEX DATABASE postgres;"
    )

    # -----------------------------------
    # Set up materialized view and search
    # -----------------------------------

    # generate computed tables and full-text-search
    print("\n[Setting up full-text search]")
    with database.session_scope(database.Session) as db_session:
        search_setup(db_session)

    # -------------
    # Final summary
    # -------------

    # Print row counts for each table.
    tqdm.write("\n[Table Statistics]")
    with database.session_scope(database.Session) as db_session:
        # Via https://stackoverflow.om/a/2611745/5004662.
        SUMMARY_SQL = """
        select table_schema,
            table_name,
            (xpath('/row/cnt/text()', xml_count))[1]::text::int as row_count
        from (
        select table_name, table_schema,
                query_to_xml(format('select count(*) as cnt from %I.%I', table_schema, table_name), false, true, '') as xml_count
        from information_schema.tables
        where table_schema = 'public' --<< change here for the schema you want
        ) t
        """

        result = db_session.execute(SUMMARY_SQL)
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

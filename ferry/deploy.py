import argparse
import collections
import csv
from typing import List

import sqlalchemy
from sqlalchemy import MetaData, schema
from sqlalchemy.sql.schema import ForeignKeyConstraint

from ferry import config, database
from ferry.includes.tqdm import tqdm

"""
This script checks the database invariants.
"""


def listing_invariants(session):
    """
    Check invariant: listing.season_code == course.season_code if listing.course_id == course.course_id.
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
    Check invariant: evaluation_questions.options is null iff evaluation_questions.is_narrative = True
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
    Check invariant: all questions sharing a tag also share is_narrative and len(options)
    """
    # Dictionary of question_code -> (is_narrative, len(options))
    tag_cache = {}

    def optlen(l):
        return len(l) if l else -1

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
    Invariant: every course should have at least one listing.
    """
    courses_no_listings = (
        session.query(database.Course)
        .select_from(database.Listing)
        .join(database.Listing.course, isouter=True)
        .group_by(database.Course.course_id)
        .having(sqlalchemy.func.count(database.Listing.listing_id) == 0)
    ).all()

    if courses_no_listings:
        raise database.InvariantError(
            f"the following courses have no listings: {', '.join(str(course) for course in courses_no_listings)}"
        )


def search_setup(session):
    """
    Setup materialized view and search function
    """

    with open(f"{config.RESOURCE_DIR}/search.sql") as f:
        sql = f.read()
    session.execute(sql)


if __name__ == "__main__":
    all_items = [
        listing_invariants,
        course_invariants,
        question_invariants,
        question_tag_invariant,
    ]

    def _match(name):
        for fn in all_items:
            if fn.__name__ == name:
                return fn
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

    print("\n[Replacing old tables with staged]")

    conn = database.Engine.connect()
    meta = MetaData(bind=database.Engine)
    meta.reflect()

    # keep track of main table constraints and indexes
    # because staged tables do not have foreign key relationships
    constraints = []
    indexes = []

    replace = conn.begin()
    conn.execute("SET CONSTRAINTS ALL DEFERRED;")

    # drop and update tables in reverse dependnecy order
    for table in meta.sorted_tables[::-1]:
        if not table.name.endswith("_staged"):
            print(f"Updating table {table.name}")
            for index in table.indexes:
                indexes.append(index)
            for constraint in table.constraints:
                constraints.append(constraint)
            # remove the old table if it is present before
            conn.execute(f"DROP TABLE IF EXISTS {table.name}_old;")
            # rename current main table to _old
            conn.execute(f'ALTER TABLE "{table.name}" RENAME TO {table.name}_old;')
            # rename staged table to main
            conn.execute(f'ALTER TABLE "{table.name}_staged" RENAME TO {table.name};')

    replace.commit()

    # check invariants
    try:

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

            with database.session_scope(database.Session) as session:
                fn(session)

        print("All invariants passed")

    # if invariant checking fails, revert the replacements
    except:

        print("Invariant checking failed. Reverting tables.")

        revert = conn.begin()
        conn.execute("SET CONSTRAINTS ALL DEFERRED;")

        for table in meta.sorted_tables[::-1]:
            if not table.name.endswith("_staged"):
                print(f"Reverting table {table.name}")
                conn.execute(
                    f'ALTER TABLE "{table.name}" RENAME TO {table.name}_staged;'
                )
                conn.execute(f'ALTER TABLE "{table.name}_old" RENAME TO {table.name};')

        revert.commit()
        raise

    print("\n[Deleting temporary old tables]")
    meta.reflect()  # update the models to reflect postgres
    delete = conn.begin()
    conn.execute("SET CONSTRAINTS ALL DEFERRED;")
    for table in meta.sorted_tables[::-1]:
        if table.name.endswith("_old"):
            print(f"Dropping table {table.name}")
            conn.execute(f"DROP TABLE IF EXISTS {table.name} CASCADE;")

    delete.commit()

    # add back the foreign key constraints
    print("\n[Regenerating constraints]")
    regen_constraints = conn.begin()
    for constraint in constraints:
        if isinstance(constraint, ForeignKeyConstraint):
            conn.execute(schema.AddConstraint(constraint))
    regen_constraints.commit()

    def index_exists(name):
        result = conn.execute(
            f"SELECT exists(SELECT 1 from pg_indexes where indexname = '{name}') as ix_exists;"
        ).first()
        return result.ix_exists

    print("\n[Regenerating indexes]")
    regen_indexes = conn.begin()
    for index in indexes:
        if index_exists(index.name):
            conn.execute(schema.DropIndex(index))
        conn.execute(schema.CreateIndex(index))
    regen_indexes.commit()

    print("\n[Reindexing]")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute("VACUUM;")
    conn.execution_options(isolation_level="AUTOCOMMIT").execute(
        "REINDEX DATABASE postgres;"
    )

    # generate computed tables and full-text-search
    print("\n[Setting up search]")
    with database.session_scope(database.Session) as session:
        search_setup(session)

    # Print row counts for each table.
    tqdm.write("\n[Table Statistics]")
    with database.session_scope(database.Session) as session:
        # Via https://stackoverflow.om/a/2611745/5004662.
        sql = """
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

        result = session.execute(sql)
        for table_counts in result:
            print(f"{table_counts[1]:>25} - {table_counts[2]:6} rows")

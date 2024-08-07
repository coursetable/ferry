import csv
from io import StringIO

import pandas as pd
from sqlalchemy import MetaData

from ferry.database import Database, Base


class DatabaseStagingError(Exception):
    pass


def stage(tables: dict[str, pd.DataFrame], database: Database):
    staged_tables = {
        f"{table_name}_staged": table for table_name, table in tables.items()
    }

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
        if table.name not in staged_tables:
            raise ValueError(
                f"{table.name} defined in Base metadata, but there is no data for it."
            )
        # create in-memory buffer for DataFrame
        buffer = StringIO()
        staged_tables[table.name].to_csv(
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

        cursor = connection.cursor()

        try:
            cursor.copy_from(
                buffer,
                table.name,
                columns=staged_tables[table.name].columns,
                sep="\t",
                null="NULL",
            )
        except Exception as error:
            connection.rollback()
            cursor.close()
            raise DatabaseStagingError from error

        # print(f"Successfully copied {table_name}")
        cursor.close()
    connection.commit()

    print("\033[F", end="")
    print("Adding new staging tables... ✔")

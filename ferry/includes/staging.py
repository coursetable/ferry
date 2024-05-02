"""
Functions for staging tables from CSVs to the Postgres database.

Used by /ferry/stage.py.
"""
import csv
from io import StringIO

import pandas as pd


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

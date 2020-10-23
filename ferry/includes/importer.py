import pandas as pd
from sqlalchemy import inspect

from ferry import config, database


def get_table(table: str):
    return pd.read_sql_table(table, con=database.Engine)


def get_all_tables(select_schemas: list):

    tables = []

    inspector = inspect(database.Engine)
    schemas = inspector.get_schema_names()

    select_schemas = [x for x in schemas if x in select_schemas]

    for schema in select_schemas:

        schema_tables = inspector.get_table_names(schema=schema)

        tables = tables + schema_tables

    tables = {table: get_table(table) for table in tables}

    return tables

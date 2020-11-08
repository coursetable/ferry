from functools import reduce
from itertools import combinations
from typing import Dict, FrozenSet, Iterable, List, Tuple, TypeVar

import networkx
import pandas as pd
from sqlalchemy import inspect

from ferry import config, database


def flatten_list_of_lists(list_of_lists):
    """
    Flatten a list of lists into a single list.

    Parameters
    ----------
    list_of_lists : list of lists

    Returns
    -------
    flattened: flattened list

    """

    flattened = [x for y in list_of_lists for x in y]

    return flattened


def merge_overlapping(sets: List[FrozenSet]) -> List:
    """
    Given a list of sets, merge sets with
    a nonempty intersection until all sets
    are disjoint

    Parameters
    ----------
    sets: input list of sets

    Returns
    -------
    sets: output list of merged sets

    """

    # deduplicate sets to improve performance
    sets = list(set(sets))

    g = networkx.Graph()
    for sub_set in sets:
        for edge in combinations(list(sub_set), 2):
            g.add_edge(*edge)

    merged = networkx.connected_components(g)
    merged = [set(x) for x in merged]

    return merged


def invert_dict_of_lists(d: Dict) -> Dict:
    """
    Given a dictionary mapping x -> [a, b, c],
    invert such that it now maps all a, b, c -> x.
    If same value in multiple keys, then inverted
    dictionary overwrites arbitrarily.

    Parameters
    ----------
    d: input dictionary of lists

    Returns
    -------
    inverted: output inverted dictionary

    """

    inverted = {}

    for k, v in d.items():
        for x in v:
            inverted[x] = k

    return inverted


N = TypeVar("N", int, float)


def elementwise_sum(a: List[N], b: List[N]) -> List[N]:
    """
    Given two lists of equal length, return
    a list of elementwise sums

    Parameters
    ----------
    a: list
        first list
    b: list
        second list

    Returns
    -------
    sums: elementwise sums

    """

    assert len(a) == len(b), "a and b must have same size"

    return [sum(x) for x in zip(a, b)]


def category_average(categories: List[int]) -> Tuple[float, int]:
    """
    Given a list-like of n category counts,
    aggregate and return the average where each
    category denotes counts of [1,2,...n]

    Parameters
    ----------
    categories: list-like
        categories

    Returns
    -------
    average: average category
    n: total number of responses

    """

    if len(categories) == 0:
        return 0, -1

    categories_sum = sum([categories[i] * (i + 1) for i in range(len(categories))])

    n = sum(categories)

    average = categories_sum / n

    return average, n


def resolve_potentially_callable(val):
    if callable(val):
        return val()
    return val


def get_table(table: str):
    """
    Read one of the tables from the database
    into Pandas dataframe (assuming SQL storage format)

    Parameters
    ----------
    table: name of table to retrieve

    Returns
    -------
    Pandas DataFrame

    """

    return pd.read_sql_table(table, con=database.Engine)


def get_table_columns(table, not_class=False):
    """
    Get column names of a table, where table is
    a SQLalchemy model or object (e.g. ferry.database.models.Course)

    Parameters
    ----------
    table: name of table to retrieve
    not_class: if the table is not a class (for instance, a junction table)

    Returns
    -------
    list of column names

    """

    if not_class:
        return [column.key for column in table.columns]

    return [column.key for column in table.__table__.columns]


def get_all_tables(select_schemas: List[str]) -> Dict:
    """
    Get all the tables under given schemas as a dictionary
    of Pandas dataframes

    Parameters
    ----------
    select_schemas: schemas to retrieve tables for

    Returns
    -------
    Dictionary of Pandas DataFrames

    """

    tables = []

    # inspect and get schema names
    inspector = inspect(database.Engine)
    schemas = inspector.get_schema_names()

    select_schemas = [x for x in schemas if x in select_schemas]

    for schema in select_schemas:

        schema_tables = inspector.get_table_names(schema=schema)

        tables = tables + schema_tables

    tables = {table: get_table(table) for table in tables}

    return tables

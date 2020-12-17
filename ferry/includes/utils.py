"""
Miscellaneous abstract utilities.
"""
import re
from itertools import combinations
from typing import Dict, FrozenSet, List, Tuple, TypeVar

import networkx
import pandas as pd
from sqlalchemy import inspect

from ferry import database


def convert_unicode(text):
    """
    Replace unicode exceptions

    Parameters
    ----------
    text: string

    Returns
    -------
    properly formatted text
    """

    # handle incorrectly coded em dash

    unicode_exceptions = {
        r"\u00e2\u20ac\u201c": "–",
        r"\u00c2\u00a0": "\u00a0",
        r"\u00c3\u00a7": "ç",
        r"\u00c3\u00a1": "á",
        r"\u00c3\u00a9": "é",
        r"\u00c3\u00ab": "ë",
        r"\u00c3\u00ae": "î",
        r"\u00c3\u00bc": "ü",
        r"\u00c3\u00b1": "ñ",
    }

    for bad_unicode, replacement in unicode_exceptions.items():
        text = re.sub(bad_unicode, replacement, text)

    # convert utf-8 bytestrings
    # pylint: disable=line-too-long
    # from https://stackoverflow.com/questions/5842115/converting-a-string-which-contains-both-utf-8-encoded-bytestrings-and-codepoints
    text = re.sub(
        r"[\xc2-\xf4][\x80-\xbf]+",
        lambda m: m.group(0).encode("latin1").decode("unicode-escape"),
        text,
    )

    return text


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

    sets_graph = networkx.Graph()
    for sub_set in sets:
        # if single listing, add it (does nothing if already present)
        if len(sub_set) == 1:
            sets_graph.add_node(tuple(sub_set)[0])
        # otherwise, add all pairwise listings
        else:
            for edge in combinations(list(sub_set), 2):
                sets_graph.add_edge(*edge)

    # get overlapping listings as connected components
    merged = networkx.connected_components(sets_graph)
    merged = [set(x) for x in merged]

    # handle courses with no cross-listings
    singles = networkx.isolates(sets_graph)
    merged += [{x} for x in singles]

    return merged


def invert_dict_of_lists(dict_of_lists: Dict) -> Dict:
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

    for key, val in dict_of_lists.items():
        for item in val:
            inverted[item] = key

    return inverted


Numeric = TypeVar("Numeric", int, float)


def elementwise_sum(list_a: List[Numeric], list_b: List[Numeric]) -> List[Numeric]:
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

    assert len(list_a) == len(list_b), "a and b must have same size"

    return [sum(x) for x in zip(list_a, list_b)]


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
    total: total number of responses

    """

    if len(categories) == 0:
        return 0, -1

    categories_sum = sum([categories[i] * (i + 1) for i in range(len(categories))])

    total = sum(categories)

    average = categories_sum / total

    return average, total


def resolve_potentially_callable(val):
    """
    Check if a value is callable, and return its result if so.

    """
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


def get_all_tables(select_schemas: List[str]) -> Dict[str, pd.DataFrame]:
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

    tables: List[str] = []

    # inspect and get schema names
    inspector = inspect(database.Engine)
    schemas = inspector.get_schema_names()

    select_schemas = [x for x in schemas if x in select_schemas]

    for schema in select_schemas:

        schema_tables = inspector.get_table_names(schema=schema)

        tables = tables + schema_tables

    mapped_tables = {table: get_table(table) for table in tables}

    return mapped_tables

from functools import reduce
from itertools import combinations
from typing import Dict, List, Set

import networkx


def merge_overlapping(sets: List[Set]) -> List:
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
    sets = set([frozenset(x) for x in sets])
    sets = list(sets)

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


def elementwise_sum(a: List, b: List) -> List:
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


def category_average(categories: List):
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
        return None, None

    categories = reduce(elementwise_sum, categories)

    if sum(categories) == 0:
        return None, None

    categories_sum = sum([categories[i] * (i + 1) for i in range(len(categories))])

    n = sum(categories)

    average = categories_sum / n

    return average, n


def resolve_potentially_callable(val):
    if callable(val):
        return val()
    return val

"""
Miscellaneous abstract utilities.

Used for processing course JSONs, resolving cross-listings, etc.
"""

import re
from itertools import combinations
from typing import Any


def convert_unicode(text: str) -> str:
    """
    Replace unicode exceptions.

    Parameters
    ----------
    text:
        Text to process.

    Returns
    -------
        Properly formatted text.
    """
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
        r"\u201c": '"',
        r"\u201d": '"',
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


def merge_overlapping(sets: list[frozenset[Any]]) -> list[set[Any]]:
    """
    Given a list of FrozenSets, merge sets with a nonempty intersection until all sets are disjoint.

    Parameters
    ----------
    sets:
        Input list of FrozenSets.

    Returns
    -------
    sets:
        Output list of merged sets.
    """
    import networkx

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


def to_element_index_map(dict_of_lists: list[set[Any]]) -> dict[Any, int]:
    """
    Given a list of sets, return a dictionary mapping each element to its index in the list.

    If an element is present in multiple sets, the index of the *last* set it appears in is used.
    """
    inverted: dict[Any, int] = {}

    for key, val in enumerate(dict_of_lists):
        for item in val:
            inverted[item] = key

    return inverted

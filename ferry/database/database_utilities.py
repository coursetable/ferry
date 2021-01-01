"""
Utility functions used by database.py
"""

from contextlib import contextmanager

import ujson


class InvariantError(Exception):
    """
    Object for invariant checking exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


class MissingTablesError(Exception):
    """
    Object for missing table exceptions.
    """

    # pylint: disable=unnecessary-pass
    pass


@contextmanager
def session_scope(session_context, *args, **kwargs):
    """Provide a transactional scope around a series of operations."""
    session = session_context(*args, **kwargs)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def eq_json(json_a, json_b) -> bool:
    """
    Check if two JSON objects are equal.
    """
    return ujson.dumps(json_a) == ujson.dumps(json_b)


def update_json(obj, attr, new_val):
    """
    Update a JSON-encoded attribute. Only attempts to set the attribute if the value has changed.
    """
    old_val = getattr(obj, attr)
    if not eq_json(old_val, new_val):
        setattr(obj, attr, new_val)

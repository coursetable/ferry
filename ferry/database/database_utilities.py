"""
Utility functions used by database.py
"""

from contextlib import contextmanager


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

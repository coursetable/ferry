from contextlib import contextmanager
from typing import TypeVar

import ujson

M = TypeVar("M")


class InvariantError(Exception):
    pass


@contextmanager
def session_scope(Session, *args, **kwargs):
    """Provide a transactional scope around a series of operations."""
    session = Session(*args, **kwargs)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def get_or_create(session, model: M, **kwargs) -> M:
    """Creates an object or returns the object if exists."""
    # Credit to Kevin @ StackOverflow.
    # From: http://stackoverflow.com/questions/2546207/does-sqlalchemy-have-an-equivalent-of-djangos-get-or-create
    # From: https://gist.github.com/jangeador/e7221fc3b5ebeeac9a08
    instance = session.query(model).filter_by(**kwargs).one_or_none()
    if instance:
        return instance, False
    else:
        instance = model(**kwargs)
        session.add(instance)
        return instance, True


def eq_json(a, b) -> bool:
    return ujson.dumps(a) == ujson.dumps(b)


def update_json(obj, attr, new_val):
    """
    Update a JSON-encoded attribute. Only attempts to set the attribute if the value has changed.
    """
    old_val = getattr(obj, attr)
    if not eq_json(old_val, new_val):
        setattr(obj, attr, new_val)

from contextlib import contextmanager


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


def get_or_create(session, model, **kwargs):
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

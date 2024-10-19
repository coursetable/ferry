import sqlalchemy
from contextlib import contextmanager
from .models import Base


class Database:
    def __init__(self, connect_string: str):
        super().__init__()
        self.connect_string = connect_string
        self.Engine = sqlalchemy.create_engine(
            connect_string,
            pool_size=10,
            max_overflow=2,
            pool_recycle=300,
            pool_pre_ping=True,
            pool_use_lifo=True,
            connect_args={
                "keepalives": 1,
                "keepalives_idle": 30,
                "keepalives_interval": 10,
                "keepalives_count": 5,
            },
        )
        self.Session = sqlalchemy.orm.sessionmaker(bind=self.Engine)


class InvariantError(Exception):
    pass


class MissingTablesError(Exception):
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

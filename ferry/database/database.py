"""
Database module for connecting to Postgres with SQLAlchemy
"""

import sqlalchemy

from ferry.database.database_utilities import MissingTablesError
from ferry.database.models import Base


def create_engine_and_session(connect_string: str):
    """
    Create an engine and session for a given connection string.

    Parameters
    ----------
    connect_string:
        Connection string for the database.

    Returns
    -------
    Engine and Session
    """
    Engine = sqlalchemy.create_engine(
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
    Base.metadata.create_all(Engine)

    if any(not table.endswith("_staged") for table in Base.metadata.tables):
        raise MissingTablesError("Model tables should all end with _staged")

    return Engine, sqlalchemy.orm.sessionmaker(bind=Engine)


class Database:
    def __init__(self, connect_string: str):
        self.connect_string = connect_string
        self.Engine, self.Session = create_engine_and_session(connect_string)
        self.Base = Base
        self.MissingTablesError = MissingTablesError

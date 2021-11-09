"""
Database module for connecting to Postgres with SQLAlchemy
"""
import sqlalchemy

from ferry.config import DATABASE_CONNECT_STRING
from ferry.database.database_utilities import MissingTablesError
from ferry.database.models import Base

Engine = sqlalchemy.create_engine(DATABASE_CONNECT_STRING)
Base.metadata.create_all(Engine)

if any(not table.endswith("_staged") for table in Base.metadata.tables):
    raise MissingTablesError("Model tables should all end with _staged")

Session = sqlalchemy.orm.sessionmaker(bind=Engine)

import sqlalchemy

from ferry.config import DATABASE_CONNECT_STRING
from ferry.database.database_utilities import MissingTablesError
from ferry.database.models import Base

Engine = sqlalchemy.create_engine(DATABASE_CONNECT_STRING)
Base.metadata.create_all(Engine)

if any(not table.endswith("_staged") for table in Base.metadata.tables):
    raise MissingTablesError("Model tables should all end with _staged")

Session = sqlalchemy.orm.sessionmaker(bind=Engine)


if __name__ == "__main__":
    # from sqlalchemy.dialects import mysql
    # from sqlalchemy.schema import CreateTable
    # from models import Season

    # print(CreateTable(Season.__table__).compile(dialect=mysql.dialect()))

    breakpoint()

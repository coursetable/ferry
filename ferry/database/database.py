import sqlalchemy

from ferry.config import DATABASE_CONNECT_STRING
from ferry.database.models import Base

Engine = sqlalchemy.create_engine(DATABASE_CONNECT_STRING)
Base.metadata.create_all(Engine)

Session = sqlalchemy.orm.sessionmaker(bind=Engine)


if __name__ == "__main__":
    # from sqlalchemy.dialects import mysql
    # from sqlalchemy.schema import CreateTable
    # from models import Season

    # print(CreateTable(Season.__table__).compile(dialect=mysql.dialect()))

    breakpoint()

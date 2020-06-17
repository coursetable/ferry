import sqlalchemy

from ferry.database.models import Base

_db_string = "postgresql+psycopg2://postgres:thisisapassword@localhost/postgres"
engine = sqlalchemy.create_engine(_db_string)
Base.metadata.create_all(engine)

Session = sqlalchemy.orm.sessionmaker(bind=engine)


if __name__ == "__main__":
    # from sqlalchemy.dialects import mysql
    # from sqlalchemy.schema import CreateTable
    # from models import Season

    # print(CreateTable(Season.__table__).compile(dialect=mysql.dialect()))

    breakpoint()
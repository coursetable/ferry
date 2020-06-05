import sqlalchemy
from models import Base

engine = sqlalchemy.create_engine("sqlite:///:memory:", echo=True)


Base.metadata.create_all(engine)

# from sqlalchemy.dialects import mysql
# from sqlalchemy.schema import CreateTable

# print(CreateTable(Season.__table__).compile(dialect=mysql.dialect()))

breakpoint()

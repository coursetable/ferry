from eralchemy import render_er
from database.models import Base

render_er(Base, "docs/db_diagram.png")

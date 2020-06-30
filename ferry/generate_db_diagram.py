from eralchemy import render_er

from ferry.database.models import Base

render_er(Base, "../docs/db_diagram.png")

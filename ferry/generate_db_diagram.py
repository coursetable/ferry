"""
Generates a schema diagram of the database based on the
SQLAlchemy schema.
"""
from pathlib import Path

from eralchemy2 import render_er

from ferry.database.models import Base


def generate_db_diagram(path: str | None = "docs/db_diagram.pdf"):
    """
    Generates a schema diagram of the database based on the
    SQLAlchemy schema.
    """
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    render_er(Base, path)

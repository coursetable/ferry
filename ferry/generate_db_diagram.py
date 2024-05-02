"""
Generates a schema diagram of the database based on the
SQLAlchemy schema.
"""

from pathlib import Path

from eralchemy2 import render_er

from ferry.database.models import Base


def generate_db_diagram(path: Path):
    """
    Generates a schema diagram of the database based on the
    SQLAlchemy schema.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    render_er(Base, path)

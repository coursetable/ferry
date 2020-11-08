"""
Init all submodules for import.
"""

from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect

from ferry.database.database import Engine, Session
from ferry.database.database_utilities import *
from ferry.database.models import (
    Base,
    Course,
    DemandStatistics,
    EvaluationNarrative,
    EvaluationQuestion,
    EvaluationRating,
    EvaluationStatistics,
    Listing,
    Professor,
    Season,
    course_professors,
)

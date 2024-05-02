"""
Init all submodules for import.
"""

from sqlalchemy import MetaData, Table
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import declarative_base

from ferry.database.database import Database
from ferry.database.database_utilities import *
from ferry.database.models import (  # DemandStatistics,
    Base,
    Course,
    EvaluationNarrative,
    EvaluationQuestion,
    EvaluationRating,
    EvaluationStatistics,
    Flag,
    Listing,
    Professor,
    Season,
    course_flags,
    course_professors,
)

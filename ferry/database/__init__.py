"""
Init all submodules for import.
"""

from sqlalchemy import MetaData, Table
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.inspection import inspect

from ferry.database.database import Database
from ferry.database.database_utilities import *
from ferry.database.models import (  # DemandStatistics,
    Base,
    Course,
    EvaluationNarrative,
    EvaluationQuestion,
    EvaluationRating,
    EvaluationStatistics,
    Listing,
    Professor,
    Season,
    course_professors,
)

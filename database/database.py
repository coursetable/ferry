import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    JSON,
    Boolean,
    Float,
    DateTime,
    Table,
    Index,
)
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship

engine = sqlalchemy.create_engine("sqlite:///:memory:", echo=True)

Base = declarative_base()


class TimestampMixin(object):
    pass
    """
    # The updated_at attributes may not work.
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.utcnow(),
        server_onupdate=func.utcnow(),
    )
    created_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.utcnow()
    )
    """


class Season(Base, TimestampMixin):
    __tablename__ = "seasons"
    season_code = Column(
        String(10), primary_key=True, comment="Season code (e.g. '202001')"
    )

    term = Column(
        String(10),
        comment="[computed] Season of the semester - one of spring, summer, or fall",
    )

    year = Column(Integer, comment="[computed] Year of the semester")


class Course(Base, TimestampMixin):
    __tablename__ = "courses"
    course_id = Column(Integer, primary_key=True)

    season_code = Column(
        String(10),
        ForeignKey("seasons.season_code"),
        comment="The season the course is being taught in",
        nullable=False,
    )
    season = relationship("Season", back_populates="course_list")

    areas = Column(JSON, comment="Course areas (humanities, social sciences, sciences)")
    course_home_url = Column(String, comment="Link to the course homepage")
    description = Column(String, comment="Course description")
    extra_info = Column(
        String, comment="Additional information (indicates if class has been cancelled)"
    )
    location_times = Column(
        String, comment="Key-value pairs consisting of `<location>:<list of times>`"
    )
    locations_summary = Column(
        String,
        comment='If single location, is `<location>`; otherwise is `<location> + <n_other_locations>` where the first location is the one with the greatest number of days. Displayed in the "Locations" column in CourseTable.',
    )
    num_students = Column(
        # TODO: should we remove this?
        Integer,
        comment="Student enrollment (retrieved from evaluations, not part of the Courses API)",
    )
    num_students_is_same_prof = Column(
        # TODO: should we remove this?
        Boolean,
        comment="Whether or not a different professor taught the class when it was this size",
    )
    requirements = Column(
        String, comment="Recommended requirements/prerequisites for the course"
    )
    section = Column(
        # TODO: should we remove this?
        String,
        comment="Which section the course is (each section has its own field, as returned in the original API output)",
    )
    times_long_summary = Column(
        String,
        comment='Course times and locations, displayed in the "Meets" row in CourseTable course modals',
    )
    times_summary = Column(
        # TODO: maybe this should be JSON?
        String,
        comment='Course times, displayed in the "Times" column in CourseTable',
    )
    times_by_day = Column(
        # TODO: maybe this should be JSON?
        String,
        comment="Course meeting times by day, with days as keys and tuples of `(start_time, end_time, location)`",
    )
    short_title = Column(
        String,
        comment='Shortened course title (first 29 characters + "...") if the length exceeds 32, otherwise just the title itself',
    )
    skills = Column(
        JSON,
        comment="Skills that the course fulfills (e.g. writing, quantitative reasoning, language levels)",
    )
    syllabus_url = Column(String, comment="Link to the syllabus")
    title = Column(String, comment="Complete course title")
    average_overall_rating = Column(
        Float,
        comment="[computed] Average overall course rating (from this course's evaluations, aggregated across cross-listings)",
    )
    average_workload = Column(
        Float,
        comment="[computed] Average workload rating ((from this course's evaluations, aggregated across cross-listings)",
    )


class Listing(Base):
    __tablename__ = "listings"
    listing_id = Column(Integer, primary_key=True, comment="Listing ID")

    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="Course that the listing refers to",
        nullable=False,
    )

    subject = Column(
        String,
        comment='Subject the course is listed under (e.g. "AMST")',
        nullable=False,
    )
    number = Column(
        String,
        comment='Course number in the given subject (e.g. "120" or "S120")',
        nullable=False,
    )
    course_code = Column(
        String,
        comment='[computed] subject + number (e.g. "AMST 312")',
        nullable=False,
        index=True,
    )
    section = Column(
        String,
        comment="Course section for the given subject and number",
        nullable=False,
    )
    season_code = Column(
        String(10),
        ForeignKey("seasons.season_code"),
        comment="When the course/listing is being taught, mapping to `seasons`",
        nullable=False,
    )
    crn = Column(
        Integer,
        comment="The CRN associated with this listing",
        nullable=False,
        index=True,
    )

    __table_args__ = (
        Index(
            "season_course_section_unique",
            "season_code",
            "subject",
            "number",
            "section",
            unique=True,
        ),
        Index("season_crn_unique", "season_code", "crn", unique=True),
    )


class Professor(Base):
    __tablename__ = "professors"

    professor_id = Column(Integer, comment="Professor ID", primary_key=True)
    name = Column(String, comment="Name of the professor", index=True, nullable=False)

    average_rating = Column(
        Float,
        comment='[computed] Average rating of the professor assessed via the "Overall assessment" question in courses taught',
    )


# Course-Professor junction table.
course_professors = Table(
    "course_professors",
    Base.metadata,
    Column("course_id", ForeignKey("courses.course_id"), primary_key=True),
    Column("professor_id", ForeignKey("professors.professor_id"), primary_key=True),
)

Base.metadata.create_all(engine)

# from sqlalchemy.dialects import mysql
# from sqlalchemy.schema import CreateTable

# print(CreateTable(Season.__table__).compile(dialect=mysql.dialect()))

breakpoint()

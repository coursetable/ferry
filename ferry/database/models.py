from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func
from sqlalchemy_mixins import ReprMixin, SerializeMixin

meta = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s_staged",
        "ix": "ix_%(column_0_label)s_staged",
        "uq": "uq_%(table_name)s_%(column_0_name)s_staged",
        "ck": "ck_%(table_name)s_%(constraint_name)s_staged",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s_staged",
        "pk": "pk_%(table_name)s_staged",
    }
)

Base = declarative_base(metadata=meta)


class BaseModel(Base, SerializeMixin, ReprMixin):
    __abstract__ = True
    pass


class Season(BaseModel):
    __tablename__ = "seasons_staged"
    season_code = Column(
        String(10), primary_key=True, comment="Season code (e.g. '202001')", index=True
    )

    term = Column(
        String(10),
        comment="[computed] Season of the semester - one of spring, summer, or fall",
    )

    year = Column(Integer, comment="[computed] Year of the semester")


# Course-Professor association/junction table.
course_professors = Table(
    "course_professors_staged",
    Base.metadata,
    Column(
        "course_id",
        ForeignKey("courses_staged.course_id"),
        primary_key=True,
        index=True,
    ),
    Column(
        "professor_id",
        ForeignKey("professors_staged.professor_id"),
        primary_key=True,
        index=True,
    ),
)


class Course(BaseModel):
    __tablename__ = "courses_staged"
    course_id = Column(Integer, primary_key=True, index=True)

    season_code = Column(
        String(10),
        ForeignKey("seasons_staged.season_code"),
        comment="The season the course is being taught in",
        index=True,
        nullable=False,
    )
    season = relationship("Season", backref="courses_staged", cascade="all")

    professors = relationship(
        "Professor",
        secondary=course_professors,
        back_populates="courses",
        cascade="all",
    )

    areas = Column(JSON, comment="Course areas (humanities, social sciences, sciences)")
    course_home_url = Column(String, comment="Link to the course homepage")
    description = Column(String, comment="Course description")
    school = Column(
        String, comment="School (e.g. YC, GS, MG) that the course is taught in"
    )
    credits = Column(Float, comment="Number of course credits")
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
    requirements = Column(
        String, comment="Recommended requirements/prerequisites for the course"
    )
    times_long_summary = Column(
        String,
        comment='Course times and locations, displayed in the "Meets" row in CourseTable course modals',
    )
    times_summary = Column(
        String,
        comment='Course times, displayed in the "Times" column in CourseTable',
    )
    times_by_day = Column(
        JSON,
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
    fysem = Column(
        Boolean,
        comment="True if the course is a first-year seminar. False otherwise.",
    )

    average_rating = Column(
        Float,
        comment="[computed] Historical average course rating for this course code, aggregated across all cross-listings",
    )
    average_workload = Column(
        Float,
        comment="[computed] Historical average workload rating, aggregated across all cross-listings",
    )


class Listing(BaseModel):
    __tablename__ = "listings_staged"
    listing_id = Column(Integer, primary_key=True, comment="Listing ID")

    course_id = Column(
        Integer,
        ForeignKey("courses_staged.course_id"),
        comment="Course that the listing refers to",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="listings_staged", cascade="all")

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
        index=True,
    )
    section = Column(
        String,
        comment="Course section for the given subject and number",
        nullable=False,
    )
    season_code = Column(
        String(10),
        ForeignKey("seasons_staged.season_code"),
        comment="When the course/listing is being taught, mapping to `seasons`",
        index=True,
        nullable=False,
    )
    season = relationship("Season", backref="listings_staged", cascade="all")
    crn = Column(
        Integer,
        comment="The CRN associated with this listing",
        index=True,
        nullable=False,
    )

    __table_args__ = (
        Index(
            "idx_season_course_section_unique_staged",
            "season_code",
            "subject",
            "number",
            "section",
            # unique=True,  # TODO: it seems this is not actually true
        ),
        Index("idx_season_code_crn_unique_staged", "season_code", "crn", unique=True),
    )


class Flag(BaseModel):
    # Course flags
    __tablename__ = "flags_staged"

    flag_id = Column(Integer, comment="Flag ID", primary_key=True)

    flag_text = Column(String, comment="Flag text", index=True, nullable=False)


# Course-Flag association/junction table.
course_flags = Table(
    "course_flags_staged",
    Base.metadata,
    Column(
        "course_id",
        ForeignKey("courses_staged.course_id"),
        primary_key=True,
        index=True,
    ),
    Column(
        "flag_id",
        ForeignKey("flags_staged.flag_id"),
        primary_key=True,
        index=True,
    ),
)


class DemandStatistics(BaseModel):
    # Course demand statistics.
    __tablename__ = "demand_statistics_staged"

    course_id = Column(
        Integer,
        ForeignKey("courses_staged.course_id"),
        primary_key=True,
        index=True,
        comment="The course to which these demand statistics apply",
    )
    latest_demand = Column(
        Integer,
        comment="Latest demand count",
    )
    latest_demand_date = Column(
        String,
        comment="Latest demand date",
    )
    course = relationship("Course", backref="demand_statistics_staged", cascade="all")

    demand = Column(
        JSON,
        comment="JSON dict containing demand stats by day",
    )


class Professor(BaseModel):
    __tablename__ = "professors_staged"

    professor_id = Column(Integer, comment="Professor ID", primary_key=True)
    name = Column(String, comment="Name of the professor", index=True, nullable=False)
    email = Column(String, comment="Email address of the professor", nullable=True)
    ocs_id = Column(String, comment="Professor ID used by Yale OCS", nullable=True)

    courses = relationship(
        "Course",
        secondary=course_professors,
        back_populates="professors",
        cascade="all",
    )

    average_rating = Column(
        Float,
        comment='[computed] Average rating of the professor assessed via the "Overall assessment" question in courses taught',
    )


class EvaluationStatistics(BaseModel):
    __tablename__ = "evaluation_statistics_staged"

    course_id = Column(
        Integer,
        ForeignKey("courses_staged.course_id"),
        primary_key=True,
        comment="The course associated with these statistics",
        index=True,
        nullable=False,
    )
    course = relationship(
        "Course",
        backref=backref("evaluation_statistics_staged", uselist=False),
        cascade="all",
    )

    enrollment = Column(
        Integer,
        comment="Placeholder for compatibility (previously held JSON for enrollment)",
    )
    enrolled = Column(Integer, comment="Number of students enrolled in course")
    responses = Column(Integer, comment="Number of responses")
    declined = Column(Integer, comment="Number of students who declined to respond")
    no_response = Column(Integer, comment="Number of students who did not respond")
    extras = Column(
        JSON, comment="Arbitrary additional information attached to an evaluation"
    )
    avg_rating = Column(Float, comment="[computed] Average overall rating")
    avg_workload = Column(Float, comment="[computed] Average workload rating")


class EvaluationQuestion(BaseModel):
    __tablename__ = "evaluation_questions_staged"

    question_code = Column(
        String,
        comment='Question code from OCE (e.g. "YC402")',
        primary_key=True,
        index=True,
    )
    is_narrative = Column(
        Boolean,
        comment="True if the question has narrative responses. False if the question has categorica/numerical responses",
    )
    question_text = Column(
        String,
        comment="The question text",
    )
    options = Column(
        JSON,
        comment="JSON array of possible responses (only if the question is not a narrative",
    )

    tag = Column(
        String,
        comment="[computed] Question type (used for computing ratings, since one question may be coded differently for different respondants)",
    )


class EvaluationNarrative(BaseModel):
    # Narrative evaluations data.
    __tablename__ = "evaluation_narratives_staged"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses_staged.course_id"),
        comment="The course to which this narrative comment applies",
        index=True,
        nullable=False,
    )
    course = relationship(
        "Course", backref="evaluation_narratives_staged", cascade="all"
    )
    question_code = Column(
        String,
        ForeignKey("evaluation_questions_staged.question_code"),
        comment="Question to which this narrative comment responds",
        index=True,
        nullable=False,
    )
    question = relationship(
        "EvaluationQuestion",
        backref="evaluation_narratives_staged",
        cascade="all",
    )

    comment = Column(
        String,
        comment="Response to the question",
    )


class EvaluationRating(BaseModel):
    # Categorical evaluations data.
    __tablename__ = "evaluation_ratings_staged"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses_staged.course_id"),
        comment="The course to which this rating applies",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="evaluation_ratings_staged", cascade="all")
    question_code = Column(
        String,
        ForeignKey("evaluation_questions_staged.question_code"),
        comment="Question to which this rating responds",
        index=True,
        nullable=False,
    )
    question = relationship(
        "EvaluationQuestion", backref="evaluation_ratings_staged", cascade="all"
    )

    rating = Column(
        JSON,
        comment="JSON array of the response counts for each option",
    )

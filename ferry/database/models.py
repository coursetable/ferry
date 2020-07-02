from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Table,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, relationship
from sqlalchemy.sql import func
from sqlalchemy_mixins import ReprMixin, SerializeMixin

Base = declarative_base()


class BaseModel(Base, SerializeMixin, ReprMixin):
    __abstract__ = True
    pass


class Season(BaseModel):
    __tablename__ = "seasons"
    season_code = Column(
        String(10), primary_key=True, comment="Season code (e.g. '202001')"
    )

    term = Column(
        String(10),
        comment="[computed] Season of the semester - one of spring, summer, or fall",
    )

    year = Column(Integer, comment="[computed] Year of the semester")


# Course-Professor association/junction table.
course_professors = Table(
    "course_professors",
    Base.metadata,
    Column("course_id", ForeignKey("courses.course_id"), primary_key=True),
    Column("professor_id", ForeignKey("professors.professor_id"), primary_key=True),
)


class Course(BaseModel):
    __tablename__ = "courses"
    course_id = Column(Integer, primary_key=True)

    season_code = Column(
        String(10),
        ForeignKey("seasons.season_code"),
        comment="The season the course is being taught in",
        index=True,
        nullable=False,
    )
    season = relationship("Season", backref="courses")

    professors = relationship(
        "Professor", secondary=course_professors, back_populates="courses"
    )

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
    requirements = Column(
        String, comment="Recommended requirements/prerequisites for the course"
    )
    times_long_summary = Column(
        String,
        comment='Course times and locations, displayed in the "Meets" row in CourseTable course modals',
    )
    times_summary = Column(
        String, comment='Course times, displayed in the "Times" column in CourseTable',
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


class Listing(BaseModel):
    __tablename__ = "listings"
    listing_id = Column(Integer, primary_key=True, comment="Listing ID")

    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="Course that the listing refers to",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="listings")

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
        String, comment='[computed] subject + number (e.g. "AMST 312")', index=True,
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
        index=True,
        nullable=False,
    )
    season = relationship("Season", backref="listings")
    crn = Column(
        Integer,
        comment="The CRN associated with this listing",
        index=True,
        nullable=False,
    )

    __table_args__ = (
        Index(
            "idx_season_course_section_unique",
            "season_code",
            "subject",
            "number",
            "section",
            # unique=True,  # TODO: it seems this is not actually true
        ),
        Index("idx_season_code_crn_unique", "season_code", "crn", unique=True),
    )


class Professor(BaseModel):
    __tablename__ = "professors"

    professor_id = Column(Integer, comment="Professor ID", primary_key=True)
    name = Column(String, comment="Name of the professor", index=True, nullable=False)

    courses = relationship(
        "Course", secondary=course_professors, back_populates="professors"
    )

    average_rating = Column(
        Float,
        comment='[computed] Average rating of the professor assessed via the "Overall assessment" question in courses taught',
    )


class HistoricalRating(BaseModel):
    __tablename__ = "historical_ratings"

    id = Column(Integer, primary_key=True)

    course_code = Column(String, comment='Course code (e.g. "CPSC 366"', index=True)

    # course_id = Column(
    #     Integer,
    #     ForeignKey("courses.course_id"),
    #     comment="The course associated with these statistics",
    #     index=True,
    #     nullable=False,
    # )

    professor_id = Column(
        Integer,
        ForeignKey("professors.professor_id"),
        comment="Professor ID",
        index=True,
        nullable=False,
    )

    course_rating_all_profs = Column(
        Float,
        comment="[computed] The average rating for this course code, across all professors who taught it",
    )
    course_rating_this_prof = Column(
        Float,
        comment="[computed] The average rating for this course code when taught by this professor",
    )
    course_workload = Column(
        Float,
        comment="[computed] The average workload for this course code, across all times it was taught",
    )

    __table_args__ = (
        # The (course_code, professor_id) combination should be unique.
        Index(
            "idx_historical_ratings_course_code_professor_unique",
            "course_code",
            "professor_id",
            unique=True,
        ),
    )


class EvaluationStatistics(BaseModel):
    __tablename__ = "evaluation_statistics"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="The course associated with these statistics",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref=backref("statistics", uselist=False))

    enrollment = Column(
        JSON, comment="a JSON dictionary with information about the course's enrollment"
    )
    extras = Column(
        JSON, comment="arbitrary additional information attached to an evaluation"
    )
    avg_rating = Column(Float, comment="[computed] Average overall rating")
    avg_workload = Column(Float, comment="[computed] Average workload rating")


class EvaluationQuestion(BaseModel):
    __tablename__ = "evaluation_questions"

    question_code = Column(
        String, comment='Question code from OCE (e.g. "YC402")', primary_key=True,
    )
    is_narrative = Column(
        Boolean,
        comment="True if the question has narrative responses. False if the question has categorica/numerical responses",
    )
    question_text = Column(String, comment="The question text",)
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
    __tablename__ = "evaluation_narratives"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="The course to which this narrative comment applies",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="evaluation_narratives")
    question_code = Column(
        String,
        ForeignKey("evaluation_questions.question_code"),
        comment="Question to which this narrative comment responds",
        index=True,
        nullable=False,
    )
    question = relationship("EvaluationQuestion", backref="evaluation_narratives")

    comment = Column(String, comment="Response to the question",)


class EvaluationRating(BaseModel):
    # Categorical evaluations data.
    __tablename__ = "evaluation_ratings"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="The course to which this rating applies",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="evaluation_ratings")
    question_code = Column(
        String,
        ForeignKey("evaluation_questions.question_code"),
        comment="Question to which this rating responds",
        index=True,
        nullable=False,
    )
    question = relationship("EvaluationQuestion", backref="evaluation_ratings")

    rating = Column(JSON, comment="JSON array of the response counts for each option",)

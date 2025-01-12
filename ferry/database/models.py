from sqlalchemy import (
    Boolean,
    Column,
    Float,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Table,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import backref, declarative_base, relationship
from sqlalchemy_mixins import ReprMixin, SerializeMixin

meta = MetaData(
    naming_convention={
        "ix": "ix_%(column_0_label)s",
        "uq": "uq_%(table_name)s_%(column_0_name)s",
        "ck": "ck_%(table_name)s_%(constraint_name)s",
        "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
        "pk": "pk_%(table_name)s",
    }
)

Base = declarative_base(metadata=meta)


class BaseModel(SerializeMixin, ReprMixin, Base):
    """
    BaseModel class for all tables.
    """

    __abstract__ = True


class Season(BaseModel):
    """
    Seasons table.
    """

    __tablename__ = "seasons"
    season_code = Column(
        String(10),
        primary_key=True,
        comment="Season code (e.g. '202001')",
        index=True,
        nullable=False,
    )

    term = Column(
        String(10),
        comment="[computed] Season of the semester - one of spring, summer, or fall",
        nullable=False,
    )

    year = Column(
        Integer,
        comment="[computed] Year of the semester",
        nullable=False,
    )


# Course-Professor association/junction table.
course_professors = Table(
    "course_professors",
    Base.metadata,
    Column(
        "course_id",
        ForeignKey("courses.course_id"),
        primary_key=True,
        index=True,
    ),
    Column(
        "professor_id",
        ForeignKey("professors.professor_id"),
        primary_key=True,
        index=True,
    ),
)


class Course(BaseModel):
    """
    Courses table.
    """

    __tablename__ = "courses"
    course_id = Column(Integer, primary_key=True, index=True)

    season_code = Column(
        String(10),
        ForeignKey("seasons.season_code"),
        comment="The season the course is being taught in",
        index=True,
        nullable=False,
    )
    season = relationship(
        "Season",
        backref="courses",
        cascade="all",
        foreign_keys="Course.season_code",
    )
    # Note: this is purposefully not a foreign key because I can't think of a
    # use case for that. Just get all listings, and use this field to find the
    # primary one.
    primary_crn = Column(Integer, comment="CRN of the primary listing")
    section = Column(
        String,
        comment="""Course section. Note that the section number is the same for
        all cross-listings.""",
        nullable=False,
    )

    professors = relationship(
        "Professor",
        secondary=course_professors,
        back_populates="courses",
        cascade="all",
    )

    # ------------------------
    # Basic course descriptors
    # ------------------------

    title = Column(String, comment="Complete course title", nullable=False)
    description = Column(String, comment="Course description")
    requirements = Column(
        String, comment="Recommended requirements/prerequisites for the course"
    )

    # ----------------------
    # Skills, areas, credits
    # ----------------------

    skills = Column(
        JSONB,
        comment="""Skills that the course fulfills (e.g. writing,
        quantitative reasoning, language levels)""",
        nullable=False,
    )

    areas = Column(
        JSONB,
        comment="Course areas (humanities, social sciences, sciences)",
        nullable=False,
    )

    credits = Column(Float, comment="Number of course credits")

    # ----------------------
    # Additional info fields
    # ----------------------

    syllabus_url = Column(String, comment="Link to the syllabus")
    course_home_url = Column(String, comment="Link to the course homepage")
    regnotes = Column(
        String,
        comment="""Registrar's notes (e.g. preference selection links,
        optional writing credits, etc.)""",
    )
    extra_info = Column(
        String,
        comment="Additional information (indicates if class has been cancelled)",
    )
    rp_attr = Column(String, comment="Reading period notes")
    classnotes = Column(String, comment="Additional class notes")
    final_exam = Column(String, comment="Final exam information")
    fysem = Column(
        Boolean,
        comment="True if the course is a first-year seminar. False otherwise.",
    )
    sysem = Column(
        Boolean,
        comment="True if the course is a sophomore seminar. False otherwise.",
    )
    colsem = Column(
        Boolean,
        comment="True if the course is a college seminar. False otherwise.",
    )

    # ------------------------------
    # Historical identical offerings
    # ------------------------------

    same_course_id = Column(
        Integer,
        comment="""[computed] Unique ID for grouping courses by historical offering.
        All courses with a given ID are identical offerings across different semesters.
        """,
        index=True,
        nullable=False,
    )

    same_course_and_profs_id = Column(
        Integer,
        comment="""[computed] Unique ID for grouping courses by historical offering.
        All courses with a given ID are identical offerings across different semesters.
        Same as 'same_course_id' with the constraint that all courses in a group were
        taught by the same professors.
        """,
        index=True,
        nullable=False,
    )

    average_gut_rating = Column(
        Float,
        comment="""[computed] average_rating - average_workload""",
    )
    average_professor_rating = Column(
        Float,
        comment="""[computed] Average of the average ratings of all professors for this course.""",
    )
    average_rating = Column(
        Float,
        comment="""[computed] Historical average course rating for this course code,
        aggregated across all cross-listings""",
    )
    average_rating_n = Column(
        Integer,
        comment="""[computed] Number of courses used to compute `average_rating`""",
    )

    average_workload = Column(
        Float,
        comment="""[computed] Historical average workload rating,
        aggregated across all cross-listings""",
    )
    average_workload_n = Column(
        Integer,
        comment="""[computed] Number of courses used to compute `average_workload`""",
    )

    average_rating_same_professors = Column(
        Float,
        comment="""[computed] Historical average course rating for this course code,
        aggregated across all cross-listings with same set of professors""",
    )
    average_rating_same_professors_n = Column(
        Integer,
        comment="""[computed] Number of courses used to compute
        `average_rating_same_professors`""",
    )

    average_workload_same_professors = Column(
        Float,
        comment="""[computed] Historical average workload rating,
        aggregated across all cross-listings with same set of professors""",
    )
    average_workload_same_professors_n = Column(
        Integer,
        comment="""[computed] Number of courses used to compute
        `average_workload_same_professors`""",
    )

    # -----------------------
    # Last-offered statistics
    # -----------------------

    last_offered_course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="""[computed] Most recent previous offering of
        course (excluding future ones)""",
        index=True,
    )

    last_offered_course = relationship(
        "Course",
        backref="courses",
        cascade="all",
        remote_side="Course.course_id",
        foreign_keys="Course.last_offered_course_id",
    )

    last_enrollment_course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="[computed] Course from which last enrollment offering was pulled",
        index=True,
    )

    last_enrollment_course = relationship(
        "Course",
        backref="courses",
        cascade="all",
        remote_side="Course.course_id",
        foreign_keys="Course.last_enrollment_course_id",
    )

    last_enrollment = Column(
        Integer,
        comment="[computed] Number of students enrolled in last offering of course",
    )

    last_enrollment_season_code = Column(
        String(10),
        ForeignKey("seasons.season_code"),
        comment="[computed] Season in which last enrollment offering is from",
        index=True,
    )

    last_enrollment_season = relationship(
        "Season",
        backref="courses_",
        cascade="all",
        foreign_keys="Course.last_enrollment_season_code",
    )

    last_enrollment_same_professors = Column(
        Boolean,
        comment="""[computed] Whether last enrollment offering
        is with same professor as current.""",
    )


class Listing(BaseModel):
    """
    Listings table.
    """

    __tablename__ = "listings"
    listing_id = Column(Integer, primary_key=True, comment="Listing ID")

    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="Course that the listing refers to",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="listings", cascade="all")

    school = Column(
        String,
        comment="School (e.g. YC, GS, MG) that the course is listed under",
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
        index=True,
        nullable=False,
    )
    section = Column(
        String,
        comment="""Course section. Note that the section number is the same for
        all cross-listings.""",
        nullable=False,
    )
    season_code = Column(
        String(10),
        ForeignKey("seasons.season_code"),
        comment="When the course/listing is being taught, mapping to `seasons`",
        index=True,
        nullable=False,
    )
    season = relationship("Season", backref="listings", cascade="all")
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
        ),
        Index(
            "idx_season_code_crn_unique",
            "season_code",
            "crn",
            unique=True,
        ),
    )


class Flag(BaseModel):
    """
    Course flags table.
    """

    __tablename__ = "flags"

    flag_id = Column(Integer, comment="Flag ID", primary_key=True)

    flag_text = Column(String, comment="Flag text", index=True, nullable=False)


# Course-Flag association/junction table.
course_flags = Table(
    "course_flags",
    Base.metadata,
    Column(
        "course_id",
        ForeignKey("courses.course_id"),
        primary_key=True,
        index=True,
    ),
    Column(
        "flag_id",
        ForeignKey("flags.flag_id"),
        primary_key=True,
        index=True,
    ),
)


course_meetings = Table(
    "course_meetings",
    Base.metadata,
    Column(
        "course_id",
        ForeignKey("courses.course_id"),
        index=True,
    ),
    Column(
        "days_of_week",
        Integer,
        comment="Days of the week for this session. It is formed through bitwise joining all the constituents, where (1 = Sunday, 2 = Monday, 4 = Tuesday, ..., 64 = Saturday). For example, if a course meets on Monday, Wednesday, and Friday, the value would be 2 + 8 + 32 = 42.",
        nullable=False,
    ),
    Column(
        "start_time",
        String,
        comment="Start time of this meeting session",
        nullable=False,
    ),
    Column(
        "end_time",
        String,
        comment="End time of this meeting session",
        nullable=False,
    ),
    Column(
        "location_id",
        ForeignKey("locations.location_id"),
        comment="Location of this meeting session",
        index=True,
    ),
)


class Building(BaseModel):
    """
    Buildings table.
    """

    __tablename__ = "buildings"

    code = Column(
        String,
        comment="Building short code/abbreviation, as in YCS",
        index=True,
        nullable=False,
        primary_key=True,
    )
    building_name = Column(String, comment="Building full name")
    url = Column(String, comment="Yale campus map URL")


class Location(BaseModel):
    """
    Locations table.
    """

    __tablename__ = "locations"

    location_id = Column(Integer, primary_key=True)
    building_code = Column(
        String,
        ForeignKey("buildings.code"),
        comment="Building code",
        index=True,
        nullable=False,
    )
    building = relationship("Building", backref="locations", cascade="all")
    room = Column(String, comment="Room number")

    __table_args__ = (
        Index(
            "idx_building_code_room_unique",
            "building_code",
            "room",
            unique=True,
        ),
    )


class Professor(BaseModel):
    """
    Professors table.
    """

    __tablename__ = "professors"

    professor_id = Column(Integer, comment="Professor ID", primary_key=True)
    name = Column(String, comment="Name of the professor", index=True, nullable=False)
    email = Column(String, comment="Email address of the professor", nullable=True)
    # ocs_id = Column(String, comment="Professor ID used by Yale OCS", nullable=True)

    courses = relationship(
        "Course",
        secondary=course_professors,
        back_populates="professors",
        cascade="all",
    )

    courses_taught = Column(
        Integer, comment="[computed] Number of courses taught", nullable=False
    )

    average_rating = Column(
        Float,
        comment="""[computed] Average rating of the professor assessed via
        the "Overall assessment" question in courses taught""",
    )

    average_rating_n = Column(
        Integer,
        comment="""[computed] Number of courses used to compute `average_rating`""",
    )


class EvaluationStatistics(BaseModel):
    """
    Evaluation statistics table.
    """

    __tablename__ = "evaluation_statistics"

    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        primary_key=True,
        comment="The course associated with these statistics",
        index=True,
        nullable=False,
    )
    course = relationship(
        "Course",
        backref=backref("evaluation_statistics", uselist=False),
        cascade="all",
    )

    enrolled = Column(Integer, comment="Number of students enrolled in course")
    responses = Column(Integer, comment="Number of responses")
    declined = Column(Integer, comment="Number of students who declined to respond")
    no_response = Column(Integer, comment="Number of students who did not respond")
    extras = Column(
        JSONB,
        comment="Arbitrary additional information attached to an evaluation",
    )
    avg_rating = Column(Float, comment="[computed] Average overall rating")
    avg_workload = Column(Float, comment="[computed] Average workload rating")


class EvaluationQuestion(BaseModel):
    """
    Evaluation questions table.
    """

    __tablename__ = "evaluation_questions"

    question_code = Column(
        String,
        comment='Question code from OCE (e.g. "YC402")',
        primary_key=True,
        index=True,
    )
    is_narrative = Column(
        Boolean,
        comment="""True if the question has narrative responses.
        False if the question has categorica/numerical responses""",
        nullable=False,
    )
    question_text = Column(
        String,
        comment="The question text",
        nullable=False,
    )
    options = Column(
        JSONB,
        comment="JSON array of possible responses (only if the question is not a narrative)",
    )

    tag = Column(
        String,
        comment="""[computed] Question type. The 'Overall' and 'Workload' tags
        are used to compute average ratings, while others are purely for
        identification purposes. No other commonality, other than that they
        contain similar keywords, is guaranteed—for example, they may have
        different options, or even differ in being narrative or not.""",
        nullable=True,
    )


class EvaluationNarrative(BaseModel):
    """
    Evaluation narratives (written ones) table.
    """

    __tablename__ = "evaluation_narratives"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="The course to which this narrative comment applies",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="evaluation_narratives", cascade="all")
    question_code = Column(
        String,
        ForeignKey("evaluation_questions.question_code"),
        comment="Question to which this narrative comment responds",
        index=True,
        nullable=False,
    )
    question = relationship(
        "EvaluationQuestion",
        backref="evaluation_narratives",
        cascade="all",
    )
    response_number = Column(
        Integer,
        comment="The number of the response for the given course and question",
        nullable=False,
    )

    comment = Column(
        String,
        comment="Response to the question",
        nullable=False,
    )

    comment_neg = Column(
        Float,
        comment="VADER sentiment 'neg' score (negativity)",
        nullable=False,
    )

    comment_neu = Column(
        Float,
        comment="VADER sentiment 'neu' score (neutrality)",
        nullable=False,
    )

    comment_pos = Column(
        Float,
        comment="VADER sentiment 'pos' score (positivity)",
        nullable=False,
    )

    comment_compound = Column(
        Float,
        comment="VADER sentiment 'compound' score (valence aggregate of neg, neu, pos)",
        nullable=False,
    )


class EvaluationRating(BaseModel):
    """
    Evaluation ratings (categorical ones) table.
    """

    __tablename__ = "evaluation_ratings"

    id = Column(Integer, primary_key=True)
    course_id = Column(
        Integer,
        ForeignKey("courses.course_id"),
        comment="The course to which this rating applies",
        index=True,
        nullable=False,
    )
    course = relationship("Course", backref="evaluation_ratings", cascade="all")
    question_code = Column(
        String,
        ForeignKey("evaluation_questions.question_code"),
        comment="Question to which this rating responds",
        index=True,
        nullable=False,
    )
    question = relationship(
        "EvaluationQuestion", backref="evaluation_ratings", cascade="all"
    )

    rating = Column(
        JSONB,
        comment="JSON array of the response counts for each option",
        nullable=False,
    )

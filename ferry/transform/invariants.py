import pandas as pd


class InvariantError(Exception):
    pass


def check_invariants(tables: dict[str, pd.DataFrame]):
    """
    Check invariant:
    - listing.season_code == course.season_code if listing.course_id == course.course_id.
    - evaluation_questions.options is null iff evaluation_questions.is_narrative = True
    - every course should have at least one listing.
    """

    listing_with_course = tables["listings"][["course_id", "season_code"]].merge(
        tables["courses"][["course_id", "season_code"]],
        on="course_id",
        suffixes=("_listing", "_course"),
    )
    diff_season_code = listing_with_course[
        listing_with_course["season_code_listing"]
        != listing_with_course["season_code_course"]
    ]
    if not diff_season_code.empty:
        raise InvariantError(
            f"listing.season_code != course.season_code for {diff_season_code}"
        )

    courses_no_listing = ~tables["courses"]["course_id"].isin(
        tables["listings"]["course_id"]
    )
    if courses_no_listing.any():
        raise InvariantError(
            f"courses with no listing {tables['courses']['course_id'][courses_no_listing]}"
        )

    narrative_with_options = tables["evaluation_questions"][
        (tables["evaluation_questions"]["is_narrative"] == True)
        & (tables["evaluation_questions"]["options"].notnull())
    ]
    non_narrative_without_options = tables["evaluation_questions"][
        (tables["evaluation_questions"]["is_narrative"] == False)
        & (tables["evaluation_questions"]["options"].isnull())
    ]
    if not narrative_with_options.empty:
        raise InvariantError(f"narrative with options {narrative_with_options}")
    if not non_narrative_without_options.empty:
        raise InvariantError(
            f"non-narrative without options {non_narrative_without_options}"
        )

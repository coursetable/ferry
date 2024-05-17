"""
Find historical offerings of a course.
"""

from typing import Any

import itertools
import edlib
import networkx
import pandas as pd
from tqdm import tqdm
import logging

MIN_TITLE_MATCH_LEN = 8
MIN_DESCRIPTION_MATCH_LEN = 32

MAX_TITLE_DIST = 0.25
MAX_DESCRIPTION_DIST = 0.25


def map_to_groups(
    dataframe: pd.DataFrame, left: str, right: str
) -> dict[Any, list[Any]]:
    """
    Given a DataFrame and two columns 'left' and 'right', construct dictionaries
    mapping from 'left' to list-grouped 'right' values.
    """
    return dataframe.groupby(left)[right].apply(list).to_dict()


def text_distance(text_1: str, text_2: str) -> float:
    """
    Get edit distance between two texts.

    Normalized by dividing by the length of the smaller text.
    """
    # return maximum distance if any being compared is empty
    if text_1 == "" or text_2 == "":
        return 1

    # make sure the shorter text comes first for infix (HW) edit distance
    if len(text_1) > len(text_2):
        text_1, text_2 = text_2, text_1

    # use the infix alignment, where gaps at start/end are not penalized
    # see https://github.com/Martinsos/edlib#alignment-methods
    raw_dist = edlib.align(text_1, text_2, mode="HW")["editDistance"]

    min_len = min(len(text_1), len(text_2))

    normal_dist = raw_dist / min_len

    return normal_dist


def is_same_course(
    title_1: str, title_2: str, description_1: str, description_2: str
) -> bool:
    """
    Based on titles and descriptions, judge if two courses are the same.
    """
    # if titles or descriptions match, consider the courses to be the same
    # give short-title / short-description courses the benefit of the doubt
    if title_1 == title_2 and title_1 != "":
        return True
    if description_1 == description_2 and description_1 != "":
        return True
    if title_1 == "" and title_2 == "" and description_1 == "" and description_2 == "":
        return True

    # otherwise, have to look at fuzzy distance
    # if titles are slightly similar, then consider two courses to be the same
    title_dist = text_distance(title_1, title_2)
    if title_dist <= MAX_TITLE_DIST:
        return True

    # if descriptions are slightly similar, then consider two courses to be the same
    description_dist = text_distance(description_1, description_2)
    if description_dist <= MAX_DESCRIPTION_DIST:
        return True

    return False


def resolve_historical_courses(
    courses: pd.DataFrame, listings: pd.DataFrame
) -> tuple[pd.Series, dict[int, list[int]]]:
    """
    Among courses, identify historical offerings of a course.

    This is equivalent to constructing a partition of course_ids such that each
    partition contains the same courses, offered over different terms.

    Returns
    -------
    same_course_id:
        Mapping from course_id to resolved same_course id, with title/description filtering.
    same_course_to_courses:
        Mapping from resolved same_course id to group of identical courses, with
        title/description filtering.
    """
    # map course to codes and code to courses
    course_to_codes = map_to_groups(listings, "course_id", "course_code")
    code_to_courses = map_to_groups(listings, "course_code", "course_id")

    # map course_id to all other courses with overlapping codes
    # flatten courses with overlapping codes
    courses_shared_code = courses.index.to_series().apply(
        lambda id_: set(
            itertools.chain.from_iterable(
                code_to_courses[code] for code in course_to_codes[id_]
            )
        )
    )

    # construct initial graph of courses:
    # each node is a unique course from the 'courses' table, and two courses are
    # linked if they share a common course code
    #
    # edges are then pruned for title/description match
    same_courses = networkx.Graph()

    # fill in the nodes first to keep courses with no same-code edges
    for course_id in courses.index:
        same_courses.add_node(course_id)

    # filter out titles and descriptions for matching
    course_to_title = courses["title"][
        courses["title"].apply(len) >= MIN_TITLE_MATCH_LEN
    ].to_dict()
    course_to_description = courses["description"][
        courses["description"].apply(len) >= MIN_DESCRIPTION_MATCH_LEN
    ].to_dict()

    for course_1, shared_code_courses in tqdm(
        courses_shared_code.items(),
        total=len(courses_shared_code),
        desc="Populating same-courses graph",
        leave=False,
    ):
        for course_2 in shared_code_courses:
            title_1 = course_to_title.get(course_1, [""])[0]
            title_2 = course_to_title.get(course_2, [""])[0]

            description_1 = course_to_description.get(course_1, [""])[0]
            description_2 = course_to_description.get(course_2, [""])[0]

            # if title and description are similar enough, keep the edge
            if is_same_course(title_1, title_2, description_1, description_2):
                same_courses.add_edge(course_1, course_2)

    logging.debug(f"Pruned shared-code edges: {same_courses.number_of_edges()}")

    # get overlapping listings as connected components
    connected_codes = networkx.connected_components(same_courses)
    connected_codes = [list(x) for x in connected_codes]

    # handle courses with no cross-listings
    singles = networkx.isolates(same_courses)
    connected_codes += [[x] for x in singles]

    # map courses to unique same-courses ID, and map same-courses ID to courses
    connected_courses = pd.Series(connected_codes, name="course_id")
    connected_courses.index.rename("same_course_id", inplace=True)
    same_course_to_courses = connected_courses.to_dict()

    # map course_id to same-course partition ID
    same_course_id = (
        connected_courses.explode()
        .reset_index(drop=False)
        .set_index("course_id")["same_course_id"]
    )

    return same_course_id, same_course_to_courses


def split_same_professors(
    same_course_id: pd.Series,
    course_to_professors: pd.Series,
) -> tuple[pd.Series, dict[int, list[int]]]:
    """
    Split an equivalent-courses partitioning further by same-professor.

    Parameters
    ----------
    same_course_id:
        Mapping from course_id to a unique identifier for each group of same-courses, produced by
        resolve_historical_courses.
    course_to_professors:
        Series mapping from course_id to frozen sets of professor_ids.

    Returns
    -------
    same_course_and_profs_id:
        Mapping from course_id to resolved same_course id,
    same_prof_course_to_courses:
        Mapping from resolved same_course id to group of identical courses.
    """
    # initialize same-courses with same-professors mapping
    same_course_profs = same_course_id.reset_index(drop=False)

    # map each course to frozenset of professors
    same_course_profs["professors"] = same_course_profs["course_id"].map(
        lambda x: course_to_professors.get(x, frozenset())
    )

    # group by previous same-course partition, then split by same-professors, and
    # reset the index to obtain a partitioning by same-course and same-professors
    professors_grouped = (
        same_course_profs.groupby(["same_course_id", "professors"])["course_id"]
        .apply(list)
        .reset_index()
    )

    professors_grouped.index.rename("same_course_and_profs_id", inplace=True)

    same_prof_course_to_courses = professors_grouped["course_id"].to_dict()

    same_course_and_profs_id = (
        professors_grouped.explode("course_id")
        .reset_index(drop=False)
        .set_index("course_id")["same_course_and_profs_id"]
    )

    return same_course_and_profs_id, same_prof_course_to_courses

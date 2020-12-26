"""
Find historical offerings of a course. Used by transform_compute.py
"""

from typing import Any, Dict, List, Tuple

import edlib
import networkx
import pandas as pd

from ferry.includes.tqdm import tqdm
from ferry.includes.utils import flatten_list_of_lists

MIN_TITLE_MATCH_LEN = 8
MIN_DESCRIPTION_MATCH_LEN = 32

MAX_TITLE_DIST = 0.25
MAX_DESCRIPTION_DIST = 0.5


def map_to_groups(
    dataframe: pd.DataFrame, left: str, right: str
) -> Dict[Any, List[Any]]:

    """
    Given a dataframe and two columns 'left' and 'right', construct dictionaries
    mapping from 'left' to list-grouped 'right' values and vice-versa.

    Parameters
    ----------
    dataframe:
        host dataframe
    left:
        name of 'left' column
    right:
        name of 'right' column

    Returns
    -------
    left_to_right and right_to_left mappings

    """

    left_to_right = dataframe.groupby(left)[right].apply(list).to_dict()  # type: ignore
    right_to_left = dataframe.groupby(right)[left].apply(list).to_dict()  # type: ignore

    return left_to_right, right_to_left


def is_same_course(
    title_1: str, title_2: str, description_1: str, description_2: str
) -> bool:

    """
    Based on titles and descriptions, judge if two courses are the same.

    Parameters
    ----------
    title_1:
        title of first course
    title_2:
        title of second course
    description_1:
        description of first course
    description_2:
        description of second course

    Returns
    -------
    Whether or not the courses are judged to be the same
    """

    def text_distance(text_1: str, text_2: str) -> float:

        """
        Get edit distance between two texts, normalized to [0,1] range
        by dividing by the length of the longer text.
        """

        raw_dist = edlib.align(text_1, text_2)["editDistance"]

        max_len = max(len(text_1), len(text_2))

        normal_dist = raw_dist / max_len

        return normal_dist

    # if titles or descriptions match, consider the courses to be the same
    # give short-title / short-description courses the benefit of the doubt
    if title_1 == title_2 or description_1 == description_2:

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
) -> Tuple[Dict[int, int], Dict[int, List[int]]]:
    """
    Among courses, identify historical offerings of a course. This is equivalent to
    constructing a partition of course_ids such that each partition contains the same
    courses, offered over different terms.

    Parameters
    ----------
    courses:
        'courses' table
    listings:
        'listings' table

    Returns
    -------
    course_to_same_course:
        mapping from course_id to resolved same_course id
    same_course_to_courses:
        mapping from resolved same_course id to group of identical courses
    """

    # map course to codes and code to courses
    course_to_codes, code_to_courses = map_to_groups(
        listings, "course_id", "course_code"
    )

    courses["codes"] = courses["course_id"].apply(course_to_codes.get)
    courses["courses_shared_code"] = courses["codes"].apply(
        lambda x: [code_to_courses[code] for code in x]
    )
    courses["courses_shared_code"] = courses["courses_shared_code"].apply(
        lambda x: list(set(flatten_list_of_lists(x)))
    )

    # filter out titles and descriptions for matching
    long_titles = courses[courses["title"].fillna("").apply(len) >= MIN_TITLE_MATCH_LEN]
    long_descriptions = courses[
        courses["description"].fillna("").apply(len) >= MIN_DESCRIPTION_MATCH_LEN
    ]

    course_to_title, _ = map_to_groups(long_titles, "course_id", "title")
    course_to_description, _ = map_to_groups(
        long_descriptions, "course_id", "description"
    )

    # construct initial graph of courses:
    # each node is a unique course from the 'courses' table, and two courses are
    # linked if they share a common course code
    #
    # edges are then pruned for title/description match

    same_courses = networkx.Graph()

    # fill in the nodes first to keep courses with no same-code edges
    for course_id in courses["course_id"]:
        same_courses.add_node(course_id)

    def add_edge_same_code(course_row):
        course_id = course_row["course_id"]

        for other_course_id in course_row["courses_shared_code"]:
            same_courses.add_edge(course_id, other_course_id)

    print("Constructing initial same-courses graph")
    courses.progress_apply(add_edge_same_code, axis=1)

    # filtered same-courses graph:
    # we iterate over edges of the same_courses graph and keep the ones that satisfy
    # our title/description matching criteria
    same_courses_filtered = networkx.Graph()

    # fill in the nodes first to keep courses with no same-code edges
    for course_id in courses["course_id"]:
        same_courses_filtered.add_node(course_id)

    for course_1, course_2 in tqdm(
        same_courses.edges(data=False), desc="Building filtered same-courses graph"
    ):

        title_1 = course_to_title.get(course_1, [""])[0]
        title_2 = course_to_title.get(course_2, [""])[0]

        description_1 = course_to_description.get(course_1, [""])[0]
        description_2 = course_to_description.get(course_2, [""])[0]

        if is_same_course(title_1, title_2, description_1, description_2):

            same_courses_filtered.add_edge(course_1, course_2)

    print(f"Original shared-code edges: {same_courses.number_of_edges()}")
    print(f"Pruned shared-code edges: {same_courses_filtered.number_of_edges()}")

    print("Identifying same courses by connected components")

    # get overlapping listings as connected components
    connected_codes = networkx.connected_components(same_courses_filtered)
    connected_codes = [list(x) for x in connected_codes]

    # handle courses with no cross-listings
    singles = networkx.isolates(same_courses_filtered)
    connected_codes += [[x] for x in singles]

    # map courses to unique same-courses ID, and map same-courses ID to courses

    connected_courses = pd.Series(connected_codes, name="course_id")
    same_course_to_courses = connected_courses.to_dict()

    same_courses_explode = connected_courses.explode()
    course_to_same_course = dict(
        zip(same_courses_explode.values, same_courses_explode.index)
    )

    return course_to_same_course, same_course_to_courses

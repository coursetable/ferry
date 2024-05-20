"""
Find historical offerings of a course.
"""

from typing import cast, Any

import logging
import itertools
import edlib
import networkx as nx
import pandas as pd
from tqdm import tqdm

# TODO: I don't quite like these hard-coded constants. Is there a better way
# to measure similarity that's more context-aware?
MIN_TITLE_MATCH_LEN = 8
MIN_DESCRIPTION_MATCH_LEN = 32

MAX_TITLE_DIST = 0.25
MAX_DESCRIPTION_DIST = 0.25

# These are courses that changed code; we only consider course pairs that have
# overlapping codes because we can't afford a full pairwise text comparison.
# These have to be added as special cases.
# We do so by pretending that the specified course codes have an extra cross-
# listing, so they can be grouped into the same shared-code component for further
# title comparison.
# TODO: is there a data structure that allows us to find similar texts quickly?
code_changes = [
    ("ENGL 121", "ENGL 421"),
    ("CPSC 427", "CPSC 327"),
]

subject_changes = [
    ("G&G", "EPS"),
]

# These are course titles that are very similar but considering them equal would
# link a lot of unwanted courses, or those that are not so similar but still the
# same course.
# Note: to disconnect two courses you have to make sure there's NO path between
# them, but to connect two you just need one edge
forced_title_comparison = {
    # These departments have basically redesigned their codes and created a bunch
    # of extra links
    ("Elementary Akkadian", "Elementary Akkadian I"): False,
    ("Elementary Akkadian I", "Elementary Akkadian II"): False,
    ("ElementaryModernStandardArabic", "Elementary Modern Standard Arabic I"): False,
    (
        "Elementary Modern Standard Arabic I",
        "Elementary Modern Standard Arabic II",
    ): False,
    **{
        tuple(sorted((n1, n2))): False
        for n1, n2 in itertools.combinations(
            [
                "Intermediate Modern Standard Arabic II",
                "Advanced Modern Standard Arabic II",
                "Arabic Seminar",
                "Intermediate Modern Standard Arabic I",
                "Advanced Modern Standard Arabic I",
                "Arabic Seminar: Early Adab",
            ],
            2,
        )
        if {n1, n2} != {"Arabic Seminar", "Arabic Seminar: Early Adab"}
    },
    **{
        tuple(sorted((n1, n2))): False
        for n1, n2 in itertools.combinations(
            [
                "Intermediate Classical Arabic II",
                "Intermediate Classical Arabic I",
                "Beginning Classical Arabic II",
                "Beginning Classical Arabic I",
            ],
            2,
        )
    },
    # Skewed by the long logistical paragraph in description
    ("Film and History", "The Idea of the Western Hemisphere"): False,
    # This is why the text comparison algorithm doesn't work
    ("Film and History", "Foxes, Hedgehogs, and History"): False,
    (
        "A Global History of the Second World War, 1937–1945",
        "History of the Body",
    ): False,
    ("Equality", "Political Economy of Gender Inequality"): False,
}


def map_to_groups(
    dataframe: pd.DataFrame, left: str, right: str
) -> dict[Any, list[Any]]:
    """
    Given a DataFrame and two columns 'left' and 'right', construct dictionaries
    mapping from 'left' to list-grouped 'right' values.
    """
    return dataframe.groupby(left)[right].apply(list).to_dict()


def distance_in_bounds(text_1: str, text_2: str, max_dist: float) -> float:
    """
    Get edit distance between two texts.

    Normalized by dividing by the length of the smaller text.
    """
    # return maximum distance if any being compared is empty
    if text_1 == "" or text_2 == "":
        return 0 if text_1 == text_2 else -1

    # make sure the shorter text comes first for infix (HW) edit distance
    if len(text_1) > len(text_2):
        text_1, text_2 = text_2, text_1

    # use the infix alignment, where gaps at start/end are not penalized
    # see https://github.com/Martinsos/edlib#alignment-methods
    # Note: character-based alignment is important. Course title are often
    # not full words especially for legacy classes or they may have no spaces
    # in between. Word splitting would overpenalize these differences.
    raw_dist = edlib.align(text_1, text_2, mode="HW", k=max_dist * len(text_1))[
        "editDistance"
    ]
    if raw_dist == -1:
        return raw_dist

    return raw_dist / len(text_1)


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
    # Discussions are never considered in the same-course relationship since they don't
    # have ratings anyway. To give them a same_course_id, we add them later
    # TODO: we should have a dedicated discussion -> course relationship
    discussions = ~listings["section"].str.isnumeric() | listings[
        "course_code"
    ].str.endswith("D")
    discussion_course_ids = listings["course_id"][discussions]
    listings = listings.loc[~discussions]

    # map course to codes and code to courses
    course_to_codes = cast(
        dict[int, list[str]], map_to_groups(listings, "course_id", "course_code")
    )
    code_to_courses = cast(
        dict[str, list[int]], map_to_groups(listings, "course_code", "course_id")
    )

    # Create a graph for all course codes that have ever been cross-listed.
    # Consider the following case: in season 1, course X has code A; in season 2,
    # it has code A and B; in season 3, it has code B. It turns out that seasons 1
    # and 3 offered the same content but season 2 was different. We will fail to
    # link them if we only consider explicitly declared cross-listings. (This sounds
    # hypothetical but it may happen with seminars, where every season a different
    # set of topics are offered.)
    cross_listed_codes = nx.Graph()

    for course_codes in course_to_codes.values():
        if len(course_codes) == 1:
            cross_listed_codes.add_node(course_codes[0])
        for code_1, code_2 in itertools.pairwise(course_codes):
            cross_listed_codes.add_edge(code_1, code_2)

    for code_1, code_2 in code_changes:
        cross_listed_codes.add_edge(code_1, code_2)

    for subject_1, subject_2 in subject_changes:
        for code_1 in code_to_courses.keys():
            if code_1.startswith(subject_1):
                code_2 = code_1.replace(subject_1, subject_2)
                if code_2 in code_to_courses:
                    cross_listed_codes.add_edge(code_1, code_2)

    cross_listed_codes = cast(
        list[set[str]], list(nx.connected_components(cross_listed_codes))
    )

    if logging.DEBUG >= logging.root.level:
        log_file = open("same_courses.log", "w")
    else:
        log_file = None

    # filter out titles and descriptions for matching
    course_to_season = courses["season_code"].to_dict()
    course_to_title = courses["title"][
        courses["title"].apply(len) >= MIN_TITLE_MATCH_LEN
    ].to_dict()
    course_to_description = courses["description"][
        courses["description"].apply(len) >= MIN_DESCRIPTION_MATCH_LEN
    ].to_dict()

    same_courses: list[list[int]] = []

    for codes in tqdm(
        cross_listed_codes,
        total=len(cross_listed_codes),
        desc="Populating same-courses graph",
        leave=False,
    ):
        course_set = set(
            itertools.chain.from_iterable(code_to_courses[c] for c in codes)
        )
        # Our goal is to connect each course to a component. We don't have to
        # connect it to every same course, just enough so we can map out the
        # connected components.
        # We first connect the graph into same-title components: this alone should
        # reduce a lot of pairwise comparisons
        titles: dict[str, list[int]] = {}
        for course_id in course_set:
            title = course_to_title.get(course_id, "")
            if title not in titles:
                titles[title] = [course_id]
            else:
                titles[title].append(course_id)

        title_components = [(i, t, c) for i, (t, c) in enumerate(titles.items())]
        # There's no title variation, nothing to match
        if len(title_components) == 1:
            same_courses.append(list(course_set))
            continue
        same_course_graph = nx.Graph()
        # fill in the nodes first to keep courses with no same-code edges
        for course_id in course_set:
            same_course_graph.add_node(course_id)
        for i, title, ids in title_components:
            for id1, id2 in itertools.pairwise(ids):
                same_course_graph.add_edge(id1, id2)
        if log_file:
            log_file.write("-" * 80 + "\n")
            log_file.write(f"Processing group {codes}\n")
            for i, title, ids in title_components:
                log_file.write(
                    f"{i}. {title} - {[course_to_season[course] + ' ' + '/'.join(course_to_codes[course]) for course in ids]}\n"
                )

        # Try to connect as many title components as possible
        for (i1, title1, ids1), (
            i2,
            title2,
            ids2,
        ) in itertools.combinations(title_components, 2):
            # If the two components are already connected, no need to do anything
            if nx.has_path(same_course_graph, ids1[0], ids2[0]):
                continue
            p = tuple(sorted((title1, title2)))
            if p in forced_title_comparison:
                res = forced_title_comparison[p]
                if log_file:
                    log_file.write(
                        f"{i1} ~ {i2}: forced {'equal' if res else 'unequal'}\n"
                    )
                if res:
                    same_course_graph.add_edge(ids1[0], ids2[0])
                continue
            title_dist = distance_in_bounds(title1, title2, MAX_TITLE_DIST)
            # Connect components with similar titles
            if title_dist != -1:
                same_course_graph.add_edge(ids1[0], ids2[0])
                if log_file:
                    log_file.write(f"{i1} ~ {i2}: title match {title_dist}\n")
                continue
            # For components with dissimilar titles: connect as long as there
            # exist two courses with similar (non-empty) descriptions
            for id1, id2 in itertools.product(ids1, ids2):
                if id1 in course_to_description and id2 in course_to_description:
                    desc_dist = distance_in_bounds(
                        course_to_description[id1],
                        course_to_description[id2],
                        MAX_DESCRIPTION_DIST,
                    )
                    if desc_dist != -1:
                        same_course_graph.add_edge(id1, id2)
                        if log_file:
                            log_file.write(
                                f"{i1} ~ {i2}: description match {desc_dist}\n"
                            )
                        break

        for x in nx.connected_components(same_course_graph):
            if log_file:
                course_code_set = set(frozenset(course_to_codes[c]) for c in x)
                for c1, c2 in itertools.combinations(course_code_set, 2):
                    if c1.isdisjoint(c2):
                        log_file.write(
                            f"[WARNING] {'/'.join(c1)} and {'/'.join(c2)} have no code in common\n"
                        )
            same_courses.append(list(x))

    for course in set(discussion_course_ids):
        same_courses.append([course])

    # map courses to unique same-courses ID, and map same-courses ID to courses
    connected_courses = pd.Series(same_courses, name="course_id")
    connected_courses.index.rename("same_course_id", inplace=True)
    same_course_to_courses = connected_courses.to_dict()

    # map course_id to same-course partition ID
    same_course_id = (
        connected_courses.explode()
        .reset_index(drop=False)
        .set_index("course_id")["same_course_id"]
    )
    if log_file:
        log_file.close()

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

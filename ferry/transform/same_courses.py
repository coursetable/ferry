"""
Find historical offerings of a course.
"""

import itertools
from typing import cast

import edlib
import pandas as pd
from scipy.cluster.hierarchy import DisjointSet
from tqdm import tqdm

# TODO: I don't quite like these hard-coded constants. Is there a better way
# to measure similarity that's more context-aware?
MIN_TITLE_MATCH_LEN = 8
MIN_DESCRIPTION_MATCH_LEN = 32

MAX_TITLE_DIST = 0.25
MAX_DESCRIPTION_DIST = 0.25

subject_changes = {
    "G&G": "EPS",
    "STAT": "S&DS",
}

# Courses that simultaneous changed codes and titles cannot be captured by our
# matching.
code_changes = {
    "ENGL 115": "ENGL 1015",
    "ENGL 121": "ENGL 1021",
    "CPSC 679": "CPSC 410",
    "CHNS 154": "CHNS 158",
    "CHNS 155": "CHNS 159",
    "LING 380": "LING 3800",
}

# The titles are too generic, so even if the prof is the same, we should not
# consider them as the same course
do_not_merge_on_prof = [
    "directed reading",
    "directed research",
    "reading and research",
    "tutorial",
    "senior essay",
    "senior project",
]

# These courses' content changed in very subtle ways that cause too many
# other courses to be linked to be connected together.
# To address this, we only allow exact text matches for these courses.
# In general it's very hard to split a cluster once they have been grouped, so
# we need to stop the grouping before it happens.
do_not_merge_on_similar_text = {
    "HIST 164J",
    "HIST 447J",
}


# These courses are grouped together because they are part of a year-long sequence
# with similar/identical titles and descriptions. However, we still want to allow
# courses with the same code to change title. Therefore, rather than forbidding
# text matching altogether, we split them after the fact. Each entry in this list
# is called a "split spec": it is a list of groups specifying which courses should
# belong to which split group. You can either specify just a course code, or a code +
# season. For each course, it must unambiguously belong to one and only one group.
# For each origin partition, we select the first split spec where every group has
# at least one matching course.
# TODO: add user survey about whether we should actually split them
always_distinct: list[list[set[str | tuple[str, str]]]] = [
    [
        {"ARBC 110", ("ARBC 501", "201403"), ("ARBC 500", "202303"), "ARBC 1100"},
        {"ARBC 120", "ARBC 1200"},
    ],  # Elementary Modern Standard Arabic I/II
    [
        {"ARBC 130", ("ARBC 502", "202103"), "ARBC 1300"},
        {"ARBC 140", "ARBC 1400"},
    ],  # Intermediate Modern Standard Arabic I/II
    [
        {"ARBC 150", ("ARBC 504", "202103"), "ARBC 1500"},
        {"ARBC 151", "ARBC 1510"},
    ],  # Advanced Modern Standard Arabic I/II
    [
        {"ARBC 136", "ARBC 156", "ARBC 1560", "ARBC 5110"},
        {"ARBC 146", "ARBC 166", "ARBC 1660", "ARBC 5120"},
    ],  # Intermediate Classical Arabic I/II
    [{"ARBC 158"}, {"ARBC 159"}],  # Advanced Classical Arabic I/II
    [
        {"BENG 355L", "BENG 3100"},
        {"BENG 356L", "BENG 3110"},
    ],  # Physiological Systems Laboratory/Biomedical Engineering Laboratory
    [
        {"CHNS 112", "CHNS 1120"},
        {
            "CHNS 122",
            *[("CHNS 132", f"201{year}03") for year in range(2, 8)],
            "CHNS 1220",
        },
    ],  # Elementary Modern Chinese for Heritage Speakers
    [
        {"CHNS 132", "CHNS 1320"},
        {"CHNS 142", "CHNS 1420"},
    ],  # Intermediate Modern Chinese for Heritage Speakers
    [
        {"CHNS 152", "CHNS 1520"},
        {"CHNS 153", "CHNS 1530"},
    ],  # Advanced Modern Chinese for Heritage Speakers
    [
        {"CHNS 154", "CHNS 158", "CHNS 1580"},
        {"CHNS 155", "CHNS 159", "CHNS 1590"},
    ],  # Advanced Chinese III/IV through Films and Stories
    [
        {"CHNS 156", "CHNS 1560"},
        {"CHNS 157", "CHNS 1570"},
    ],  # Advanced Modern Chinese through Film for Heritage Speakers
    [
        {"CHNS 170", "CHNS 1700", "CHNS 5700"},
        {"CHNS 171", "CHNS 1710", "CHNS 5710"},
    ],  # Introduction to Literary Chinese I/II
    [{"PHYS 180", "PHYS 1800"}, {"PHYS 181", "PHYS 1810"}],  # University Physics
    [
        {"PHYS 401", "PHYS 4010"},
        {"PHYS 402", "PHYS 4020"},
    ],  # Advanced Classical Physics from Newton to Einstein
    [
        {"PHIL 742", "PHIL 271", "LING 671", "LING 271", "LING 2710", "PHIL 2271"},
        {"PHIL 703"},
    ],  # Philosophy of Language (completely different courses)
]


def find_matching_split_spec(
    courses: set[str | tuple[str, str]],
) -> list[set[str | tuple[str, str]]] | None:
    candidates = []
    for split_spec in always_distinct:
        if all(any(course in group for course in courses) for group in split_spec):
            candidates.append(split_spec)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        raise ValueError(
            f"Multiple matching split specs: {candidates} for courses {courses}"
        )
    return None


def fix_department(code: str) -> str:
    dep, rest = code.split(" ", 1)
    if dep in subject_changes:
        return f"{subject_changes[dep]} {rest}"
    return code


def distance_in_bounds(text_1: str, text_2: str, attr: str) -> float:
    """
    Get edit distance between two texts.

    Normalized by dividing by the length of the smaller text.
    """
    text_1 = text_1.strip()
    text_2 = text_2.strip()
    # Always consider empty as unequal
    if text_1 == "" or text_2 == "":
        return -1

    if attr == "title_norm":
        max_dist = MAX_TITLE_DIST
    elif attr == "description":
        max_dist = MAX_DESCRIPTION_DIST
    else:
        raise ValueError(f"Unknown attribute: {attr}")

    # Make sure the shorter text comes first for infix (HW) edit distance
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


def reverse_map(mapping: pd.Series) -> pd.Series:
    return (
        mapping.explode()
        .reset_index()
        .groupby(cast(str, mapping.name))[mapping.index.name]
        .apply(list)
    )


def resolve_historical_courses(
    courses: pd.DataFrame, listings: pd.DataFrame, course_to_professors: pd.Series
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
    discussions = ~listings["section"].str.isdigit() | listings[
        "course_code"
    ].str.endswith("D")
    discussion_ids = listings[discussions]["course_id"].drop_duplicates()
    listings = listings[~discussions].copy()
    listings["course_code_norm"] = listings["course_code"].apply(fix_department)
    data = courses[courses.index.isin(listings["course_id"])][
        ["season_code", "title", "description"]
    ].copy()
    data["course_codes"] = listings.groupby("course_id")["course_code_norm"].apply(list)
    data["title_norm"] = data["title"].str.lower()
    data["prof_ids"] = course_to_professors
    school_listings = listings[~listings["season_code"].str.endswith("2")]
    summer_listings = listings[listings["season_code"].str.endswith("2")]
    school_data = data[~data["season_code"].str.endswith("2")]
    summer_data = data[data["season_code"].str.endswith("2")]
    tqdm.pandas(desc="Partitioning school year courses", leave=False)
    school_same_course_id = partition_same_courses(school_data, school_listings)
    tqdm.pandas(desc="Partitioning summer courses", leave=False)
    summer_same_course_id = partition_same_courses(summer_data, summer_listings)
    discussion_same_course_id = pd.Series(
        discussion_ids.values, index=discussion_ids.values
    )
    same_course_id = (
        pd.concat(
            [school_same_course_id, summer_same_course_id, discussion_same_course_id]
        )
        .sort_index()
        .rename("same_course_id")
    )
    same_course_id.index.rename("course_id", inplace=True)
    same_course_to_courses = reverse_map(same_course_id).to_dict()
    return same_course_id, same_course_to_courses


def partition_same_courses(data: pd.DataFrame, listings: pd.DataFrame) -> pd.Series:
    data = data[data.index.isin(listings["course_id"])]
    same_course_partitions = DisjointSet(elements=data.index)
    have_cross_listed = DisjointSet(elements=listings["course_code_norm"])

    def mark_cross_listed(codes: list[str]):
        for c1, c2 in itertools.pairwise(codes):
            have_cross_listed.merge(c1, c2)

    data["course_codes"].apply(mark_cross_listed)
    tqdm.pandas(desc="Merging same title courses", leave=False)
    data.groupby("title_norm").progress_apply(
        merge_same_title_courses,
        include_groups=False,
        same_course_partitions=same_course_partitions,
        have_cross_listed=have_cross_listed,
    )
    tqdm.pandas(desc="Merging same code courses", leave=False)
    listings.groupby("course_code_norm")["course_id"].progress_apply(
        merge_same_code_courses,
        include_groups=False,
        listings=listings,
        data=data,
        same_course_partitions=same_course_partitions,
    )
    return map_course_id_to_same_course_id(same_course_partitions, data)


def all_same_course(group: pd.DataFrame, same_course_partitions: DisjointSet):
    return same_course_partitions.subset(group.index[0]) >= set(group.index)


# We partition all the course IDs based on whether they were in the same
# department, or were taught by the same professor
def merge_same_title_courses(
    same_title_group: pd.DataFrame,
    same_course_partitions: DisjointSet,
    have_cross_listed: DisjointSet,
):
    dep_to_course_ids = reverse_map(
        same_title_group["course_codes"].apply(
            lambda x: [v.split(" ", 1)[0] for v in x]
        )
    )
    for course_ids in dep_to_course_ids:
        for id1, id2 in itertools.pairwise(course_ids):
            same_course_partitions.merge(id1, id2)
    subset_to_course_codes: dict[int, set[str]] = {}
    for id in same_title_group.index:
        subset = same_course_partitions[id]
        subset_to_course_codes.setdefault(subset, set()).update(
            same_title_group.loc[id, "course_codes"]
        )
    for subset1, subset2 in itertools.combinations(subset_to_course_codes, 2):
        if same_course_partitions.connected(subset1, subset2):
            continue
        all_codes1 = subset_to_course_codes[subset1]
        all_codes2 = subset_to_course_codes[subset2]
        for c1, c2 in itertools.product(all_codes1, all_codes2):
            if have_cross_listed.connected(c1, c2):
                same_course_partitions.merge(subset1, subset2)
                break
    # If all courses have been merged into the same partition, we can stop here
    if all_same_course(same_title_group, same_course_partitions):
        return
    if any(keyword in same_title_group.name for keyword in do_not_merge_on_prof):
        return
    prof_to_course_ids = reverse_map(same_title_group["prof_ids"])
    for course_ids in prof_to_course_ids:
        for id1, id2 in itertools.pairwise(course_ids):
            same_course_partitions.merge(id1, id2)


def merge_by_similar_text(
    same_code_group: pd.DataFrame,
    attr: str,
    same_course_partitions: DisjointSet,
    all_data: pd.DataFrame,
    merge_same_text: bool = True,
):
    # Merging by similar text should only cause courses from different times to be merged.
    # We avoid merging two courses in the same season.
    def merge_if_different_season(id1: int, id2: int):
        subset1 = same_course_partitions.subset(id1)
        subset2 = same_course_partitions.subset(id2)
        subset1_seasons = all_data.loc[list(subset1), "season_code"]
        subset2_seasons = all_data.loc[list(subset2), "season_code"]
        if set(subset1_seasons) & set(subset2_seasons):
            return
        same_course_partitions.merge(id1, id2)

    attr_to_course_ids = reverse_map(same_code_group[attr])
    if merge_same_text:
        for ids in attr_to_course_ids:
            for id1, id2 in itertools.pairwise(ids):
                merge_if_different_season(id1, id2)
        if all_same_course(same_code_group, same_course_partitions):
            return
    for (a, group1), (b, group2) in itertools.combinations(
        attr_to_course_ids.items(), 2
    ):
        # Courses within each group are already merged, so we only need to
        # merge the first two if needed
        if same_course_partitions.connected(group1[0], group2[0]):
            continue
        if distance_in_bounds(cast(str, a), cast(str, b), attr) >= 0:
            # print(f"Merge\n{subdata.loc[group1, ["title", "season_code", "course_codes"]]}\nand\n{subdata.loc[group2, ["title", "season_code", "course_codes"]]}\nbased on {attr}:\n{a}\n{b}")
            merge_if_different_season(group1[0], group2[0])


# Merge again based on course_codes: for each group with overlapping course_codes
# check if the titles or descriptions are similar
def merge_same_code_courses(
    same_code_ids: pd.Series,
    listings: pd.DataFrame,
    data: pd.DataFrame,
    same_course_partitions: DisjointSet,
):
    # Pretend the old courses were also offered with the new codes
    if same_code_ids.name in code_changes:
        same_code_ids = pd.concat(
            [
                same_code_ids,
                listings[listings["course_code"] == code_changes[same_code_ids.name]][
                    "course_id"
                ],
            ]
        )
    if same_code_ids.name in do_not_merge_on_similar_text:
        return
    same_code_group = data.loc[same_code_ids]
    if all_same_course(same_code_group, same_course_partitions):
        return
    merge_by_similar_text(
        same_code_group, "title_norm", same_course_partitions, data, False
    )
    if all_same_course(same_code_group, same_course_partitions):
        return
    merge_by_similar_text(same_code_group, "description", same_course_partitions, data)


def map_course_id_to_same_course_id(
    same_course_partitions: DisjointSet, data: pd.DataFrame
):
    course_id_to_same_course_id: dict[int, int] = {}
    for subset in same_course_partitions.subsets():
        subdata = data.loc[sorted(subset)]
        # Split always-distinct courses
        id_to_season_and_codes = cast(
            dict[int, set[str | tuple[str, str]]],
            subdata.apply(
                lambda row: set(
                    (code, row["season_code"]) for code in row["course_codes"]
                )
                | set(row["course_codes"]),
                axis=1,
            ).to_dict(),
        )
        all_codes = set(itertools.chain.from_iterable(id_to_season_and_codes.values()))
        split_spec = find_matching_split_spec(all_codes)
        if split_spec:
            subset_partitions = [set() for _ in range(len(split_spec))]
            for course_id in subset:
                course_codes = id_to_season_and_codes[course_id]
                overlapping_sets: list[int] = []
                for i, code_set in enumerate(split_spec):
                    if course_codes & code_set:
                        overlapping_sets.append(i)
                if len(overlapping_sets) > 1:
                    print(subdata[["season_code", "course_codes", "title"]])
                    raise ValueError(
                        f"Course ID {course_id} overlaps with multiple sets: {overlapping_sets}"
                    )
                elif len(overlapping_sets) == 1:
                    subset_partitions[overlapping_sets[0]].add(course_id)
                else:
                    print(subdata[["season_code", "course_codes", "title"]])
                    raise ValueError(
                        f"Course ID {course_id} with codes {course_codes} does not overlap with any set in the split spec {split_spec}"
                    )
        else:
            subset_partitions = [subset]
        for subset in subset_partitions:
            if not subset:
                print(subdata[["season_code", "course_codes", "title"]])
                raise ValueError(
                    f"Empty partition in {subset_partitions} split according to {split_spec}"
                )
            same_course_id = min(subset)
            for course_id in subset:
                course_id_to_same_course_id[course_id] = same_course_id
    return pd.Series(course_id_to_same_course_id)


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

    professors_grouped["same_course_and_profs_id"] = professors_grouped[
        "course_id"
    ].apply(min)
    same_prof_course_to_courses = professors_grouped.set_index(
        "same_course_and_profs_id"
    )["course_id"].to_dict()

    same_course_and_profs_id = professors_grouped.explode("course_id").set_index(
        "course_id"
    )["same_course_and_profs_id"]

    return same_course_and_profs_id, same_prof_course_to_courses

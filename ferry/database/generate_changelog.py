import pandas as pd
from pathlib import Path
from time import gmtime, strftime
from typing import cast, Any, TypedDict, Callable
import networkx as nx


class DiffRecord(TypedDict):
    deleted_rows: pd.DataFrame
    added_rows: pd.DataFrame
    # Has one extra column: columns_changed
    changed_rows: pd.DataFrame


# The keys need to be in sync with the CourseTables class in import_courses.py
primary_keys = {
    "seasons": ["season_code"],
    "courses": ["course_id"],
    "listings": ["listing_id"],
    "course_professors": ["course_id", "professor_id"],
    "professors": ["professor_id"],
    "course_flags": ["course_id", "flag_id"],
    "flags": ["flag_id"],
    "course_meetings": ["course_id", "location_id", "start_time", "end_time"],
    "locations": ["location_id"],
    "buildings": ["code"],
}

# Changes to these columns are synced to the DB, but are not recorded by last mod.
# These are purely computed columns and are subject to change based on our algorithm.
computed_columns = {
    "seasons": [],
    "courses": [
        "same_course_id",
        "same_course_and_profs_id",
        "average_gut_rating",
        "average_professor_rating",
        "average_rating",
        "average_rating_n",
        "average_workload",
        "average_workload_n",
        "average_rating_same_professors",
        "average_rating_same_professors_n",
        "average_workload_same_professors",
        "average_workload_same_professors_n",
        "last_offered_course_id",
        "last_enrollment_course_id",
        "last_enrollment",
        "last_enrollment_season_code",
        "last_enrollment_same_professors",
    ],
    "listings": [],
    "course_professors": [],
    "professors": [
        "courses_taught",
        "average_rating",
        "average_rating_n",
    ],
    "course_flags": [],
    "flags": [],
    "course_meetings": [],
    "locations": [],
    "buildings": [],
}


class PartitionDiff(TypedDict):
    merged: set[tuple[list[int], int]]
    split: set[tuple[int, list[int]]]
    exchanged: set[tuple[list[int], list[int]]]
    added: set[int]
    removed: set[int]


def create_section(depth: int, title: str, content: str, if_empty: str | None = None):
    if not content:
        if if_empty is not None:
            return f"{'#' * depth} {title}\n\n{if_empty}\n\n"
        return ""
    return f"{'#' * depth} {title}\n\n{content.strip()}\n\n"


def create_listing_link(row: pd.Series):
    return f"[{row['course_code']} {row['section']}](https://coursetable.com/catalog?course-modal={row['season_code']}-{row['crn']})"


weekdays = {
    "Su": 0,
    "M": 1,
    "T": 2,
    "W": 3,
    "Th": 4,
    "F": 5,
    "Sa": 6,
}


def print_days_of_week(days_of_week: int):
    days = []
    for day, value in weekdays.items():
        if days_of_week & (1 << value):
            days.append(day)
    res = "".join(days)
    if res == "MTWThF":
        return "M–F"
    return res


# TODO: use this to print same_course changes
def analyze_partition_diff(old_partition: pd.Series, new_partition: pd.Series):
    """
    Given two mappings from entity IDs to partition IDs, analyze how the partitions
    have changed. Changes include:
    - Partitions that were merged
    - Partitions that were split
    - Multiple partitions that exchanged entities
    - Entire partitions deleted
    - Entire partitions added
    """
    # g is a bipartite graph of old and new partitions. An edge goes from an old
    # partition to a new partition if there is an entity that was in the old partition
    # and is now in the new partition. So one old partition can map to 0 (deleted),
    # 1 (unchanged/merged), or many new partitions (split/exchanged), and one new
    # partition can map to 0 (added), 1 (unchanged/split), or many old partitions
    # (merged/exchanged).
    g = nx.Graph()
    g.add_nodes_from(old_partition.index.map(lambda v: f"{v}_old"), partition=0)
    g.add_nodes_from(new_partition.index.map(lambda v: f"{v}_new"), partition=1)
    partition_changes = pd.merge(
        old_partition, new_partition, left_index=True, right_index=True, how="inner"
    )
    for old_id, new_id in partition_changes.itertuples(index=False):
        g.add_edge(f"{old_id}_old", f"{new_id}_new")
    partition_diff: PartitionDiff = {
        "merged": set(),
        "split": set(),
        "exchanged": set(),
        "added": set(),
        "removed": set(),
    }
    for component in nx.connected_components(g):
        old_partitions = set()
        new_partitions = set()
        for node in component:
            partition_id = int(node.split("_")[0])
            if node.endswith("_old"):
                old_partitions.add(partition_id)
            else:
                new_partitions.add(partition_id)
        if len(old_partitions) == 1 and len(new_partitions) == 1:
            # Unchanged, nothing to do
            pass
        elif len(old_partitions) == 1 and len(new_partitions) > 1:
            partition_diff["split"].add((list(old_partitions)[0], list(new_partitions)))
        elif len(old_partitions) > 1 and len(new_partitions) == 1:
            partition_diff["merged"].add(
                (list(old_partitions), list(new_partitions)[0])
            )
        elif len(old_partitions) > 1 and len(new_partitions) > 1:
            partition_diff["exchanged"].add(
                (list(old_partitions), list(new_partitions))
            )
        elif len(old_partitions) == 0:
            partition_diff["added"].add(list(new_partitions)[0])
        elif len(new_partitions) == 0:
            partition_diff["removed"].add(list(old_partitions)[0])
    return partition_diff


def print_course_changes(
    changes: dict[str, tuple[Any, Any]],
    prof_info: pd.DataFrame,
    flag_info: pd.DataFrame,
    location_info: pd.DataFrame,
):
    res = ""
    for column, (old, new) in changes.items():
        if column == "professors":
            old_profs, new_profs = (
                cast(pd.DataFrame, old)["professor_id"],
                cast(pd.DataFrame, new)["professor_id"],
            )
            old_profs_info = prof_info.loc[old_profs]
            new_profs_info = prof_info.loc[new_profs]
            res += f"  - Professor: {", ".join(old_profs_info['name']) or "N/A"} → {", ".join(new_profs_info['name']) or "N/A"}\n"
        elif column == "flags":
            old_flags, new_flags = (
                cast(pd.DataFrame, old)["flag_id"],
                cast(pd.DataFrame, new)["flag_id"],
            )
            old_flags_info = flag_info.loc[old_flags]
            new_flags_info = flag_info.loc[new_flags]
            res += f"  - Flags: {", ".join(old_flags_info['flag_text']) or "N/A"} → {", ".join(new_flags_info['flag_text']) or "N/A"}\n"
        elif column == "meetings":
            old_meetings = cast(pd.DataFrame, old).apply(
                lambda row: f"{print_days_of_week(row['days_of_week'])} {row['start_time']}–{row['end_time']} {location_info.loc[row['location_id']]['name'] if not pd.isna(row['location_id']) else ''}",
                axis=1,
            )
            new_meetings = cast(pd.DataFrame, new).apply(
                lambda row: f"{print_days_of_week(row['days_of_week'])} {row['start_time']}–{row['end_time']} {location_info.loc[row['location_id']]['name'] if not pd.isna(row['location_id']) else ''}",
                axis=1,
            )
            res += f"  - Meetings: {", ".join(old_meetings) or "N/A"} → {", ".join(new_meetings) or "N/A"}\n"
        elif column == "listings":
            old_listings = cast(pd.DataFrame, old).apply(create_listing_link, axis=1)
            new_listings = cast(pd.DataFrame, new).apply(create_listing_link, axis=1)
            if old_listings.equals(new_listings):
                continue
            res += f"  - Listings: {" / ".join(old_listings) or "N/A"} → {" / ".join(new_listings) or "N/A"}\n"
        else:
            res += f"  - {column}: {old if not pd.isna(old) else "N/A"} → {new if not pd.isna(new) else "N/A"}\n"
    return res


def print_table_diff(
    table_diff: DiffRecord,
    table_old: pd.DataFrame,
    table_new: pd.DataFrame,
    identify_row: Callable[[pd.Series], str],
    table_name: str,
):
    pk = primary_keys[table_name]
    additions = "".join(table_diff["added_rows"].apply(identify_row, axis=1))
    removals = "".join(table_diff["deleted_rows"].apply(identify_row, axis=1))
    updates = ""
    for _, row in table_diff["changed_rows"].iterrows():
        update = ""
        for column in row["columns_changed"]:
            if column in computed_columns[table_name]:
                continue
            old_val = table_old[table_old[pk] == row[pk]][column].values[0]
            new_val = table_new[table_new[pk] == row[pk]][column].values[0]
            update += f"  - {column}: {old_val if not pd.isna(old_val) else 'N/A'} → {new_val if not pd.isna(new_val) else 'N/A'}\n"
        if update:
            updates += f"{identify_row(row)}{update}"
    changes = ""
    changes += create_section(3, f"Additions", additions)
    changes += create_section(3, f"Removals", removals)
    changes += create_section(3, f"Updates", updates)
    return create_section(2, f"{table_name.capitalize()} changes", changes)


def register_junction_change(
    course_id: int,
    junction_old: pd.DataFrame,
    junction_new: pd.DataFrame,
    change_name: str,
    course_id_to_changes: dict[int, dict[str, tuple[Any, Any]]],
):
    if course_id not in course_id_to_changes:
        course_id_to_changes[course_id] = {}
    course_id_to_changes[course_id][change_name] = (
        junction_old[junction_old["course_id"] == course_id],
        junction_new[junction_new["course_id"] == course_id],
    )


def register_junction_changes(
    junction_diff: DiffRecord,
    junction_old: pd.DataFrame,
    junction_new: pd.DataFrame,
    change_name: str,
    course_id_to_changes: dict[int, dict[str, tuple[Any, Any]]],
    include_columns: list[str] | None = None,
):
    for course_id in junction_diff["added_rows"]["course_id"].values:
        # Course itself was added
        if junction_old[junction_old["course_id"] == course_id].empty:
            continue
        register_junction_change(
            course_id, junction_old, junction_new, change_name, course_id_to_changes
        )
    for course_id in junction_diff["deleted_rows"]["course_id"].values:
        # Course itself was removed
        if junction_new[junction_new["course_id"] == course_id].empty:
            continue
        register_junction_change(
            course_id, junction_old, junction_new, change_name, course_id_to_changes
        )
    for row in junction_diff["changed_rows"].itertuples():
        course_id = row.course_id
        # Not a change that interests us
        if include_columns is not None and not set(include_columns).intersection(
            row.columns_changed
        ):
            continue
        register_junction_change(
            course_id, junction_old, junction_new, change_name, course_id_to_changes
        )


def print_courses_diff(
    diff: dict[str, DiffRecord],
    tables_old: dict[str, pd.DataFrame],
    tables: dict[str, pd.DataFrame],
):
    course_additions = ""
    for course in diff["courses"]["added_rows"].itertuples():
        listings = tables["listings"][
            tables["listings"]["course_id"] == course.course_id
        ]
        listings_already_exist = listings[
            listings["listing_id"].isin(tables_old["listings"]["listing_id"])
        ]
        links = listings.apply(create_listing_link, axis=1)
        course_additions += (
            f"- {course.season_code} {" / ".join(links)} {course.title}\n"
        )
        if not listings_already_exist.empty:
            course_additions += f"  - Note: {", ".join(listings_already_exist["course_code"])} already exist; this is probably due to a cross-listing split\n"
    course_removals = ""
    for course in diff["courses"]["deleted_rows"].itertuples():
        listings = tables_old["listings"][
            tables_old["listings"]["course_id"] == course.course_id
        ]
        listings_still_exist = listings[
            listings["listing_id"].isin(tables["listings"]["listing_id"])
        ]
        links = listings.apply(create_listing_link, axis=1)
        course_removals += (
            f"- {course.season_code} {" / ".join(links)} {course.title}\n"
        )
        if not listings_still_exist.empty:
            course_removals += f"  - Note: {", ".join(listings_still_exist["course_code"])} still exist; this is probably due to a cross-listing merge\n"
    course_updates = ""
    course_id_to_changes: dict[int, dict[str, tuple[Any, Any]]] = {}
    for _, course in diff["courses"]["changed_rows"].iterrows():
        course_id = cast(int, course["course_id"])
        if course_id not in course_id_to_changes:
            course_id_to_changes[course_id] = {}
        for column in course["columns_changed"]:
            if column in computed_columns["courses"]:
                continue
            course_id_to_changes[course_id][column] = (
                tables_old["courses"][tables_old["courses"]["course_id"] == course_id][
                    column
                ].values[0],
                tables["courses"][tables["courses"]["course_id"] == course_id][
                    column
                ].values[0],
            )
    register_junction_changes(
        diff["course_professors"],
        tables_old["course_professors"],
        tables["course_professors"],
        "professors",
        course_id_to_changes,
    )
    register_junction_changes(
        diff["course_flags"],
        tables_old["course_flags"],
        tables["course_flags"],
        "flags",
        course_id_to_changes,
    )
    register_junction_changes(
        diff["course_meetings"],
        tables_old["course_meetings"],
        tables["course_meetings"],
        "meetings",
        course_id_to_changes,
    )
    register_junction_changes(
        diff["listings"],
        tables_old["listings"],
        tables["listings"],
        "listings",
        course_id_to_changes,
        # Only include changes to the course_id column, because changes to
        # other columns are reported by the listings section
        ["course_id"],
    )
    prof_info = (
        tables["professors"]
        .set_index("professor_id")
        .combine_first(tables_old["professors"].set_index("professor_id"))
    )
    flag_info = (
        tables["flags"]
        .set_index("flag_id")
        .combine_first(tables_old["flags"].set_index("flag_id"))
    )
    location_info = (
        tables["locations"]
        .set_index("location_id")
        .combine_first(tables_old["locations"].set_index("location_id"))
    )
    location_info["name"] = location_info["building_code"] + " " + location_info["room"]
    for course_id, changes in course_id_to_changes.items():
        if not changes:
            continue
        listings = tables["listings"][tables["listings"]["course_id"] == course_id]
        links = listings.apply(create_listing_link, axis=1)
        course = tables["courses"][tables["courses"]["course_id"] == course_id]
        course_updates += f"- {course["season_code"].values[0]} {" / ".join(links)} {course['title'].values[0]}\n"
        course_updates += print_course_changes(
            changes, prof_info, flag_info, location_info
        )
    course_changes = ""
    course_changes += create_section(3, "Additions", course_additions)
    course_changes += create_section(3, "Removals", course_removals)
    course_changes += create_section(3, "Updates", course_updates)
    return create_section(2, "Courses changes", course_changes)


def print_diff(
    diff: dict[str, DiffRecord],
    tables_old: dict[str, pd.DataFrame],
    tables: dict[str, pd.DataFrame],
    output_dir: Path,
):
    output_dir.mkdir(parents=True, exist_ok=True)
    time = strftime("%Y-%m-%dT%H:%M:%S", gmtime())
    # TODO: use actual GH commit message
    content = f"# {time} change log\n\n"
    summary = ""
    for table_name, diff_record in diff.items():
        if (
            diff_record["added_rows"].empty
            and diff_record["deleted_rows"].empty
            and diff_record["changed_rows"].empty
        ):
            continue
        summary += f"- {table_name}\n"
        if not diff_record["added_rows"].empty:
            summary += f"  - Added: {len(diff_record['added_rows'])}\n"
        if not diff_record["deleted_rows"].empty:
            summary += f"  - Removed: {len(diff_record['deleted_rows'])}\n"
        if not diff_record["changed_rows"].empty:
            summary += f"  - Updated: {len(diff_record['changed_rows'])}\n"
    content += create_section(2, "Summary", summary, "No changes")
    content += print_table_diff(
        diff["seasons"],
        tables_old["seasons"],
        tables["seasons"],
        lambda row: f"- {row['season_code']}\n",
        "seasons",
    )
    listing_changes = ""
    for listing in diff["listings"]["changed_rows"].itertuples():
        columns_changed = [k for k in listing.columns_changed if k != "course_id"]
        if not columns_changed:
            continue
        listing_changes += f"- [{listing.season_code} {listing.course_code} {listing.section}](https://coursetable.com/catalog?course-modal={listing.season_code}-{listing.crn})\n"
        for column in columns_changed:
            listing_changes += f"  - {column}: {tables_old['listings'][tables_old['listings']['listing_id'] == listing.listing_id][column].values[0]} → {tables['listings'][tables['listings']['listing_id'] == listing.listing_id][column].values[0]}\n"
    if listing_changes:
        listing_changes = (
            "Here we only report listing information changes. Changes to their association with courses (the addition or removal of cross-listings, etc.) are reported in the courses section.\n\n"
            + listing_changes
        )
    content += create_section(2, "Listing changes", listing_changes)
    content += print_courses_diff(diff, tables_old, tables)
    content += print_table_diff(
        diff["professors"],
        tables_old["professors"],
        tables["professors"],
        lambda row: f"- {row['name']}\n",
        "professors",
    )
    content += print_table_diff(
        diff["flags"],
        tables_old["flags"],
        tables["flags"],
        lambda row: f"- {row['flag_text']}\n",
        "flags",
    )
    content += print_table_diff(
        diff["locations"],
        tables_old["locations"],
        tables["locations"],
        lambda row: f"- {row['building_code']} {row['room']}\n",
        "locations",
    )
    content += print_table_diff(
        diff["buildings"],
        tables_old["buildings"],
        tables["buildings"],
        lambda row: f"- {row['code']}\n",
        "buildings",
    )
    with open(output_dir / f"{time}.md", "w") as f:
        f.write(content)

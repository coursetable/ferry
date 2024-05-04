import os
from pathlib import Path
from typing import Any

import pandas as pd
from tqdm import tqdm

from ferry.transform.transform_compute import (
    courses_computed,
    evaluation_statistics_computed,
    professors_computed,
    questions_computed,
)
from ferry.transform.transform_import import (
    import_courses,
    import_evaluations,
)


def transform(data_dir: Path):
    """
    Import the parsed course and evaluation data into CSVs generated with Pandas.

    Used immediately before stage.py as the first step in the import process.
    """

    # ---------------------
    # Get seasons to import
    # ---------------------
    # get full list of course seasons from files
    (data_dir / "migrated_courses").mkdir(parents=True, exist_ok=True)
    course_seasons = sorted(
        [
            filename.split(".")[0]  # remove the .json extension
            for filename in set(
                os.listdir(data_dir / "migrated_courses")
                + os.listdir(data_dir / "parsed_courses")
            )
            if filename[0] != "."
        ]
    )

    # ----------------------
    # Import course listings
    # ----------------------

    print(f"\nSeason(s): {', '.join(course_seasons)}")
    print("\nImporting courses...")

    merged_course_info_: list[pd.DataFrame] = []

    for season in tqdm(course_seasons, desc="Loading course JSONs", leave=False):
        # Read the course listings, giving preference to freshly parsed over migrated ones.
        parsed_courses_file = data_dir / "parsed_courses" / f"{season}.json"
        if not parsed_courses_file.is_file():
            # check migrated courses as a fallback
            parsed_courses_file = data_dir / "migrated_courses" / f"{season}.json"
        if not parsed_courses_file.is_file():
            print(
                f"Skipping season {season}: not found in parsed or migrated courses."
            )
            continue
        parsed_course_info = pd.read_json(parsed_courses_file)
        parsed_course_info["season_code"] = season
        merged_course_info_.append(parsed_course_info)

    merged_course_info = pd.concat(merged_course_info_, axis=0).reset_index(drop=True)

    (
        courses,
        listings,
        course_professors,
        professors,
        course_flags,
        flags,
    ) = import_courses(merged_course_info, course_seasons)
    del merged_course_info

    print("\033[F", end="")
    print("Importing courses... ✔")

    print("[Summary]")
    print(f"Total courses: {len(courses)}")
    print(f"Total listings: {len(listings)}")
    print(f"Total course-professors: {len(course_professors)}")
    print(f"Total professors: {len(professors)}")
    print(f"Total course-flags: {len(course_flags)}")
    print(f"Total flags: {len(flags)}")

    # -------------------------
    # Import course evaluations
    # -------------------------

    print("\nImporting course evaluations...")

    (
        evaluation_narratives,
        evaluation_ratings,
        evaluation_statistics,
        evaluation_questions,
    ) = import_evaluations(data_dir / "parsed_evaluations", listings)

    # define seasons table for import
    seasons = pd.DataFrame(
        [int(x) for x in course_seasons], columns=["season_code"], dtype=int
    )
    seasons["term"] = seasons["season_code"].apply(
        lambda x: {"1": "spring", "2": "summer", "3": "fall"}[str(x)[-1]]
    )
    seasons["year"] = (
        seasons["season_code"].astype(str).apply(lambda x: x[:4]).astype(int)
    )

    print("\033[F", end="")
    print("Importing course evaluations... ✔")

    print("[Summary]")
    print(f"Total evaluation narratives: {len(evaluation_narratives)}")
    print(f"Total evaluation ratings: {len(evaluation_ratings)}")
    print(f"Total evaluation statistics: {len(evaluation_statistics)}")
    print(f"Total evaluation questions: {len(evaluation_questions)}")

    # ----------------------------
    # Compute secondary attributes
    # ----------------------------

    print("\nComputing secondary attributes...")

    evaluation_questions = questions_computed(evaluation_questions)

    evaluation_statistics = evaluation_statistics_computed(
        evaluation_statistics, evaluation_ratings, evaluation_questions
    )

    courses = courses_computed(
        courses, listings, evaluation_statistics, course_professors
    )

    professors = professors_computed(
        professors, course_professors, evaluation_statistics
    )

    print("\033[F", end="")
    print("Computing secondary attributes... ✔")

    # -----------------------------
    # Output tables to disk as CSVs
    # -----------------------------

    print("\nWriting tables to disk as CSVs...")

    csv_dir = data_dir / "importer_dumps"
    csv_dir.mkdir(parents=True, exist_ok=True)

    csvs = {
        "seasons": seasons,
        "courses": courses,
        "listings": listings,
        "professors": professors,
        "course_professors": course_professors,
        "flags": flags,
        "course_flags": course_flags,
        # "discussions": discussions,
        # "course_discussions": course_discussions,
        # "demand_statistics": demand_statistics,
        "evaluation_questions": evaluation_questions,
        "evaluation_narratives": evaluation_narratives,
        "evaluation_ratings": evaluation_ratings,
        "evaluation_statistics": evaluation_statistics,
    }

    def export_csv(
        table: pd.DataFrame,
        table_name: str,
        csv_kwargs: dict[str, Any] | None = None,
    ):
        """
        Exports a table to a CSV file with provided name.

        Parameters
        ----------
        table:
            table to export
        table_name:
            name of table to export
        csv_kwargs:
            additional arguments to pass to export function
        """

        if csv_kwargs is None:
            csv_kwargs = {}

        table.to_csv(csv_dir / f"{table_name}.csv", **csv_kwargs)

    for table_name, table in csvs.items():
        export_csv(table, table_name)

    print("\033[F", end="")
    print("Writing tables to disk as CSVs... ✔")

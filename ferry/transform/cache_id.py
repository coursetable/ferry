import pandas as pd
import numpy as np
from pathlib import Path
from ferry.crawler.cache import save_cache_json, load_cache_json


def merge_id_cache(
    cache_path: Path, data: dict[str, int], throw_on_conflict: bool = True
):
    existing_data = load_cache_json(cache_path) or {}
    if throw_on_conflict:
        for key, value in data.items():
            if key in existing_data and existing_data[key] != value:
                raise ValueError(
                    f"ID cache conflict: {key} is already assigned to {existing_data[key]}, changing it to {value}"
                )
    existing_data.update(data)
    save_cache_json(cache_path, existing_data)


def save_id_cache(tables: dict[str, pd.DataFrame], data_dir: Path):
    """
    Each table in the course tables has one (or more) unique ID column.
    To ensure that these IDs stay consistent across multiple runs, we cache
    them based on alternative unique keys. We don't use those composite keys
    as PKs in the DB because numeric primary keys are faster.

    We cache the following:
    - `courses`: `course_id`, `same_course_id`, `same_course_and_profs_id`
    - `listings`: `listing_id`
    - `flags`: `flag_id`
    - `professors`: `professor_id`
    - `evaluation_narratives`: `id`
    - `evaluation_ratings`: `id`

    We can alternatively locate every row by:
    - `courses`: all its related listings (`listing_id`)
    - `listings`: season + CRN
    - `flags`: flag text
    - `professors`: name + email
    - `evaluation_narratives`: course + question + response number
    - `evaluation_ratings`: course + question

    This one is not affected by the `use_cache` option: it always saves the cache.
    This is because this is not for performance, but for consistency.
    """
    cache_dir = data_dir / "id_cache"
    cache_dir.mkdir(exist_ok=True)

    course_to_id = (
        tables["listings"].set_index(["season_code", "crn"])["course_id"].to_dict()
    )
    course_to_id = {f"{k[0]}-{k[1]}": v for k, v in course_to_id.items()}
    same_course_to_id = (
        tables["courses"].set_index("course_id")["same_course_id"].to_dict()
    )
    same_course_to_id = {str(k): v for k, v in same_course_to_id.items()}
    same_course_and_profs_to_id = (
        tables["courses"].set_index("course_id")["same_course_and_profs_id"].to_dict()
    )
    same_course_and_profs_to_id = {
        str(k): v for k, v in same_course_and_profs_to_id.items()
    }
    listing_to_id = (
        tables["listings"].set_index(["season_code", "crn"])["listing_id"].to_dict()
    )
    listing_to_id = {f"{k[0]}-{k[1]}": v for k, v in listing_to_id.items()}
    flag_to_id = tables["flags"].set_index("flag_text")["flag_id"].to_dict()
    location_to_id = (
        tables["locations"].set_index(["code", "number"])["location_id"].to_dict()
    )
    location_to_id = {f"{k[0]} {k[1]}": v for k, v in location_to_id.items()}
    professor_to_id = (
        tables["professors"].set_index(["name", "email"])["professor_id"].to_dict()
    )
    professor_to_id = {
        f"{k[0]} <{k[1]}>" if str(k[1]) != "nan" else k[0]: v
        for k, v in professor_to_id.items()
    }

    narrative_to_id = (
        tables["evaluation_narratives"]
        .set_index(["course_id", "question_code", "response_number"])["id"]
        .to_dict()
    )
    narrative_to_id = {f"{k[0]}-{k[1]}-{k[2]}": v for k, v in narrative_to_id.items()}
    rating_to_id = (
        tables["evaluation_ratings"]
        .set_index(["course_id", "question_code"])["id"]
        .to_dict()
    )
    rating_to_id = {f"{k[0]}-{k[1]}": v for k, v in rating_to_id.items()}
    # Cross-listing status can change, so the course id may change too
    merge_id_cache(cache_dir / "course_id.json", course_to_id, False)
    merge_id_cache(cache_dir / "same_course_id.json", same_course_to_id, False)
    merge_id_cache(
        cache_dir / "same_course_and_profs_id.json", same_course_and_profs_to_id, False
    )
    merge_id_cache(cache_dir / "listing_id.json", listing_to_id)
    merge_id_cache(cache_dir / "flag_id.json", flag_to_id)
    merge_id_cache(cache_dir / "location_id.json", location_to_id)
    merge_id_cache(cache_dir / "professor_id.json", professor_to_id)
    merge_id_cache(cache_dir / "evaluation_narrative_id.json", narrative_to_id, False)
    merge_id_cache(cache_dir / "evaluation_rating_id.json", rating_to_id, False)

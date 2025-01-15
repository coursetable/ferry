import pandas as pd
import logging
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
    - `flags`: `flag_id`
    - `locations`: `location_id`
    - `professors`: `professor_id`

    We can alternatively locate every row by:
    - `flags`: flag text
    - `locations`: building code + room
    - `professors`: name + email

    This one is not affected by the `use_cache` option: it always saves the cache.
    This is because this is not for performance, but for consistency.
    """
    cache_dir = data_dir / "id_cache"
    cache_dir.mkdir(exist_ok=True)

    flag_to_id = tables["flags"].set_index("flag_text")["flag_id"].to_dict()
    location_to_id = (
        tables["locations"]
        .set_index(["building_code", "room"])["location_id"]
        .to_dict()
    )
    location_to_id = {
        f"{k[0]} {'' if pd.isna(k[1]) else k[1]}": v for k, v in location_to_id.items()
    }
    professor_to_id = (
        tables["professors"].set_index(["name", "email"])["professor_id"].to_dict()
    )
    professor_to_id = {
        f"{k[0]} <{k[1]}>" if str(k[1]) != "nan" else k[0]: v
        for k, v in professor_to_id.items()
    }
    never_used: dict[str, str] = {}
    for key in professor_to_id:
        if "<" in key:
            name_only_version = key.split("<")[0].strip()
            if name_only_version in professor_to_id:
                never_used[name_only_version] = key
    if never_used:
        logging.warning(
            f"The following professor IDs will never be used because they will always be replaced with the more specific version: {never_used}"
        )
        for name_only_version in never_used.keys():
            del professor_to_id[name_only_version]

    merge_id_cache(cache_dir / "flag_id.json", flag_to_id)
    merge_id_cache(cache_dir / "location_id.json", location_to_id)
    merge_id_cache(cache_dir / "professor_id.json", professor_to_id)

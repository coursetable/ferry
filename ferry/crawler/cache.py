from pathlib import Path
from typing import Any


def load_cache_json(path: Path):
    """
    Load JSON from cache file

    Parameters
    ----------
    path: str
        Path to cache file

    Returns
    -------
    data: Any | None
        JSON data
    """
    import ujson

    if not path.is_file():
        return None

    with open(path, "r") as f:
        return ujson.load(f)


def save_cache_json(
    path: Path,
    data: Any,
    indent: int = 4,
):
    """
    Save JSON to cache file

    Parameters
    ----------
    path: str
        Path to cache file
    data: Any
        Must be JSON serializable
    indent: int = 4
        Indentation for JSON file
    """
    import ujson

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, "w") as f:
        ujson.dump(data, f, indent=indent)

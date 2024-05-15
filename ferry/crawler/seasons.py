"""
Fetches a list of all seasons from the Yale Courses API.
"""

import re

from httpx import AsyncClient
from pathlib import Path
from .cache import load_cache_json, save_cache_json

# -----------------------------------------
# Retrieve seasons from unofficial Yale API
# -----------------------------------------


async def fetch_seasons(
    data_dir: Path, client: AsyncClient, use_cache: bool
) -> list[str]:
    print("Fetching course seasons...", end=" ")
    if (
        use_cache
        and (cache_load := load_cache_json(data_dir / "course_seasons.json"))
        is not None
    ):
        return cache_load
    r = await client.get("https://courses.yale.edu/")

    # Successful response
    if r.status_code == 200:
        course_seasons = re.findall(r'option value="(\d{6})"', r.text)

        # exclude '999999' and '999998' catch-all 'Past seasons' season option
        course_seasons = sorted(
            [x for x in course_seasons if x != "999999" and x != "999998"]
        )

        save_cache_json(data_dir / "course_seasons.json", course_seasons)
        print("✔")

        return course_seasons

    # Unsuccessful
    else:
        print("✘")
        raise SystemExit(f"Unsuccessful course seasons response: code {r.status_code}")

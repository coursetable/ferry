"""
Fetches a list of all seasons from the Yale Courses API.
"""

import re

import ujson
from httpx import AsyncClient
from pathlib import Path

# -----------------------------------------
# Retrieve seasons from unofficial Yale API
# -----------------------------------------


async def fetch_seasons(data_dir: Path, client: AsyncClient):
    print("Fetching course seasons...", end=" ")
    r = await client.get("https://courses.yale.edu/")

    # Successful response
    if r.status_code == 200:
        course_seasons = re.findall(r'option value="(\d{6})"', r.text)

        # exclude '999999' and '999998' catch-all 'Past seasons' season option
        course_seasons = sorted(
            [x for x in course_seasons if x != "999999" and x != "999998"]
        )

        with open(data_dir / "course_seasons.json", "w") as f:
            ujson.dump(course_seasons, f, indent=4)

        print("✔")

        return course_seasons

    # Unsuccessful
    else:
        print("✘")
        raise SystemExit(f"Unsuccessful course seasons response: code {r.status_code}")

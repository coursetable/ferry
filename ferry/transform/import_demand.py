import logging
from pathlib import Path

import pandas as pd

from ferry.crawler.cache import load_cache_json


def match_demand_to_listings(
    demand: pd.DataFrame, listings: pd.DataFrame
) -> pd.DataFrame:
    """
    Resolve each demand row's listing label (e.g. "CENG 1200 01") to a real
    listing_id. A demand page fetched through any one cross-listed
    subject/number shows every cross-listed section's counts, so the same
    listing ends up described by multiple parsed_demand entries - this is
    just identity resolution + dedup against the listings table classes
    already built, not the connected-components logic classes needs, since
    demand data is already keyed by (subject, number, section).
    """
    listing_lookup: dict[tuple[str, str, str, str], int] = {
        (row.season_code, row.subject, row.number, row.section): row.listing_id
        for row in listings.itertuples()
    }

    def resolve_listing_id(row: pd.Series) -> int | None:
        parts = row["listing"].split(" ")
        if len(parts) < 3:
            return None
        subject = parts[0]
        section = parts[-1].lstrip("0") or "0"
        number = " ".join(parts[1:-1])
        return listing_lookup.get((row["season_code"], subject, number, section))

    demand["listing_id"] = demand.apply(resolve_listing_id, axis=1)

    unmatched = demand[demand["listing_id"].isna()]
    if len(unmatched) > 0:
        logging.warning(
            f"Could not match {len(unmatched)} demand rows to a listing "
            f"(e.g. {unmatched['listing'].unique()[:5].tolist()})"
        )

    demand = demand.dropna(subset=["listing_id"])
    demand["listing_id"] = demand["listing_id"].astype(int)

    return demand


def import_demand(
    data_dir: Path, seasons: list[str], listings: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Import course demand statistics from JSON files in parsed_demand.

    Returns
    -------
    listing_demand: corresponds to database.ListingDemandStatistics
    course_demand: corresponds to database.CourseDemandStatistics - sum of
        listing_demand across every listing in each cross-listing group
    """
    print("\nImporting course demand statistics...")
    parsed_demand_dir = data_dir / "parsed_demand"

    rows: list[dict] = []
    for season in seasons:
        season_demand = load_cache_json(parsed_demand_dir / f"{season}.json")
        if season_demand is None:
            continue
        for course in season_demand:
            for count in course["counts"]:
                rows.append(
                    {
                        "season_code": season,
                        "listing": count["listing"],
                        "date": count["date"],
                        "registered": count["registered"],
                        "waitlisted": count["waitlisted"],
                        "visiting": count["visiting"],
                    }
                )

    demand = pd.DataFrame(
        rows,
        columns=["season_code", "listing", "date", "registered", "waitlisted", "visiting"],
    )

    if len(demand) > 0:
        demand = match_demand_to_listings(demand, listings)

        # Multiple parsed_demand entries (one per cross-listed subject we
        # fetched through) describe the same underlying listings - drop dupes.
        demand = demand.drop_duplicates(subset=["listing_id", "date"], keep="first")

        demand["date"] = pd.to_datetime(demand["date"]).dt.date
        listing_demand = demand[
            ["listing_id", "date", "registered", "waitlisted", "visiting"]
        ]

        course_demand = (
            listing_demand.merge(
                listings[["listing_id", "course_id"]], on="listing_id"
            )
            .groupby(["course_id", "date"], as_index=False)[
                ["registered", "waitlisted", "visiting"]
            ]
            .sum()
        )
    else:
        listing_demand = pd.DataFrame(
            columns=["listing_id", "date", "registered", "waitlisted", "visiting"]
        )
        course_demand = pd.DataFrame(
            columns=["course_id", "date", "registered", "waitlisted", "visiting"]
        )

    listing_demand = listing_demand.reset_index(drop=True)
    listing_demand.index.rename("id", inplace=True)

    print("\033[F", end="")
    print("Importing course demand statistics... ✔")
    print(
        f"Total demand statistics: {len(listing_demand)} listing-level, "
        f"{len(course_demand)} course-level"
    )

    return listing_demand, course_demand

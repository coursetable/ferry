import pandas as pd

from ferry.transform.import_courses import resolve_cross_listings


def _listing(crn: int, crns: set[int]) -> dict:
    return {
        "season_code": "202701",
        "crn": crn,
        "crns": crns,
        "school": "YC",
        "course_code": f"HSHM {crn}",
        "section": "01",
        "primary_crn": crn,
    }


def test_incomplete_cross_listing_fix_ignores_missing_crns():
    listings = pd.DataFrame(
        [
            _listing(22008, {22008, 22010}),
            _listing(22010, {22008, 22010}),
            _listing(22011, {22011}),
        ]
    )
    resolved, courses = resolve_cross_listings(listings)
    assert len(courses) == 1
    assert resolved["course_id"].nunique() == 1


def test_incomplete_cross_listing_fix_connects_all_listed_crns():
    listings = pd.DataFrame(
        [
            _listing(24287, {24287, 24351}),
            _listing(24288, {24288, 24352}),
            _listing(24351, {24287, 24351, 24352}),
            _listing(24352, {24288, 24351, 24352}),
        ]
    )
    resolved, courses = resolve_cross_listings(listings)
    assert len(courses) == 1
    assert resolved["course_id"].nunique() == 1

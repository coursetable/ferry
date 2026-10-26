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
    # The override for 202701 names CRN 22828, but it is absent from the data;
    # linking to it would leave a dangling reference in the cross-list graph
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

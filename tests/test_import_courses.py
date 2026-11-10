import json
from pathlib import Path

import pandas as pd
import pytest

from ferry.database.models import Course, Listing
from ferry.transform.import_courses import _read_parsed_season, resolve_cross_listings


def _write_parsed_season(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(json.dumps(rows))


def _listing(crn: int, crns: set[int], section_full: bool = False) -> dict:
    return {
        "season_code": "202701",
        "crn": crn,
        "crns": crns,
        "school": "YC",
        "course_code": f"HSHM {crn}",
        "section": "01",
        "primary_crn": crn,
        "section_full": section_full,
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


def test_cross_listings_keep_independent_section_full_status():
    listings = pd.DataFrame(
        [
            _listing(22008, {22008, 22010}, section_full=False),
            _listing(22010, {22008, 22010}, section_full=True),
        ]
    )
    resolved, courses = resolve_cross_listings(listings)
    assert len(courses) == 1
    assert resolved["section_full"].tolist() == [False, True]


def test_section_full_is_a_listing_column_not_a_course_column():
    assert "section_full" in Listing.__table__.columns
    assert "section_full" not in Course.__table__.columns


def test_old_parsed_season_without_section_full_defaults_to_false(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    path = tmp_path / "202501.json"
    _write_parsed_season(path, [{"crn": 1}])
    parsed = _read_parsed_season(path, "202501")
    assert parsed["season_code"].tolist() == ["202501"]
    assert parsed["section_full"].tolist() == [False]
    assert parsed["section_full"].dtype == bool
    assert "202501" in caplog.text
    assert "section_full" in caplog.text


def test_old_parsed_season_with_missing_section_full_values_defaults_to_false(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
):
    path = tmp_path / "202501.json"
    _write_parsed_season(path, [{"crn": 1, "section_full": None}])
    parsed = _read_parsed_season(path, "202501")
    assert parsed["section_full"].tolist() == [False]
    assert parsed["section_full"].dtype == bool
    assert "202501" in caplog.text
    assert "section_full" in caplog.text


def test_parsed_season_preserves_section_full(tmp_path: Path):
    path = tmp_path / "202603.json"
    _write_parsed_season(
        path,
        [
            {"crn": 1, "section_full": True},
            {"crn": 2, "section_full": False},
        ],
    )
    parsed = _read_parsed_season(path, "202603")
    assert parsed["season_code"].tolist() == ["202603", "202603"]
    assert parsed["section_full"].tolist() == [True, False]
    assert parsed["section_full"].dtype == bool

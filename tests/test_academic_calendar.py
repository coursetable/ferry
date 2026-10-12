import asyncio
import unittest
from unittest import mock

import httpx

from ferry.crawler.academic_calendar import fetch_with_retries
from ferry.crawler.academic_calendar.parse import (
    CalendarParseError,
    parse_academic_calendar,
)


def make_page(sections: dict[str, list[tuple[str, str, str]]]) -> str:
    body = ""
    for heading, rows in sections.items():
        body += f"<h2>{heading}</h2>"
        body += '<table class="sc_sctable tbl_calterm"><tbody>'
        for date, day, description in rows:
            body += f"<tr><td>{date}</td><td>{day}</td><td>{description}</td></tr>"
        body += "</tbody></table>"
    return (
        "<html><body><h1>Yale College Programs of Study 2026&#8211;2027</h1>"
        f"<div id='textcontainer'>{body}</div></body></html>"
    )


class AcademicCalendarParseTests(unittest.TestCase):
    def test_parses_terms_and_infers_years(self) -> None:
        html = make_page(
            {
                "FALL 2026": [
                    ("Sept. 2", "W", "Fall classes begin."),
                    ("Jan. 4", "M", "Fall-term final grades due."),
                ],
                "SPRING 2027": [
                    ("Jan. 19", "T", "Spring-term classes begin."),
                    ("Mar. 13–Mar. 28", "S–Su", "Spring recess."),
                ],
                "Summer Session 2026": [],
            }
        )
        calendar = parse_academic_calendar(html, source_url="https://example.com")

        self.assertEqual(calendar["academic_year"], "2026-2027")
        self.assertEqual(
            [(e["season_code"], e["date"], e["end_date"]) for e in calendar["events"]],
            [
                ("202603", "2026-09-02", None),
                ("202603", "2027-01-04", None),
                ("202701", "2027-01-19", None),
                ("202701", "2027-03-13", "2027-03-28"),
            ],
        )

    def test_rejects_day_of_week_mismatch(self) -> None:
        html = make_page({"FALL 2026": [("Sept. 2", "M", "Wrong weekday.")]})
        with self.assertRaises(CalendarParseError):
            parse_academic_calendar(html, source_url="https://example.com")

    def test_tolerates_occasional_day_of_week_typo(self) -> None:
        rows = [
            (f"Sept. {day}", ["M", "T", "W", "Th", "F", "S", "Su"][(day - 7) % 7], "x")
            for day in range(7, 28)
        ]
        rows.append(("Sept. 28", "W", "Typo, actually a Monday."))
        html = make_page({"FALL 2026": rows})
        calendar = parse_academic_calendar(html, source_url="https://example.com")
        self.assertEqual(len(calendar["events"]), 22)

    def test_parses_old_single_table_layout(self) -> None:
        html = (
            "<html><body><table class='sc_sctable tbl_caltable caltable'>"
            "<tr><th colspan='3'>Fall Term 2019</th></tr>"
            "<tr><td>Aug. 21</td><td>W</td><td>Residences open.<br/>"
            "Online Course Selection opens<br/></td></tr>"
            "<tr><th colspan='3'>Spring Term 2020</th></tr>"
            "<tr><td>Jan. 9</td><td>TH</td><td>Residences open.</td></tr>"
            "</table></body></html>"
        )
        calendar = parse_academic_calendar(html, source_url="https://example.com")

        self.assertEqual(calendar["academic_year"], "2019-2020")
        self.assertEqual(
            [
                (e["season_code"], e["date"], e["day_of_week"], e["description"])
                for e in calendar["events"]
            ],
            [
                ("201903", "2019-08-21", "W", "Residences open."),
                ("201903", "2019-08-21", "W", "Online Course Selection opens"),
                ("202001", "2020-01-09", "Th", "Residences open."),
            ],
        )

    def test_rejects_empty_calendar(self) -> None:
        with self.assertRaises(CalendarParseError):
            parse_academic_calendar(make_page({}), source_url="https://example.com")


class FetchWithRetriesTests(unittest.TestCase):
    def fetch(self, responses: list, max_retries: int) -> httpx.Response:
        calls = iter(responses)

        def handler(request: httpx.Request) -> httpx.Response:
            result = next(calls)
            if isinstance(result, Exception):
                raise result
            return httpx.Response(result)

        async def run() -> httpx.Response:
            async with httpx.AsyncClient(
                transport=httpx.MockTransport(handler)
            ) as client:
                return await fetch_with_retries(
                    client, "https://example.com", max_retries
                )

        with mock.patch("asyncio.sleep", new=mock.AsyncMock()):
            return asyncio.run(run())

    def test_retries_dropped_connection_and_server_error(self) -> None:
        r = self.fetch(
            [httpx.RemoteProtocolError("Server disconnected"), 503, 200],
            max_retries=3,
        )
        self.assertEqual(r.status_code, 200)

    def test_gives_up_after_max_retries(self) -> None:
        with self.assertRaises(httpx.RemoteProtocolError):
            self.fetch(
                [httpx.RemoteProtocolError("Server disconnected")] * 2,
                max_retries=1,
            )

    def test_does_not_retry_not_found(self) -> None:
        r = self.fetch([404], max_retries=3)
        self.assertEqual(r.status_code, 404)


if __name__ == "__main__":
    unittest.main()

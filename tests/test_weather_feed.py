"""ingestion/weather_feed.py: request planning, the answer checks and the rate-limit wait, on recorded answers."""

from datetime import date

import pytest
import requests

from ingestion import weather_feed as feed


def answer(start: date, end: date, rain: float = 1.0, temp: float = 20.0, states: int | None = None) -> list[dict]:
    days = [d.isoformat() for d in (date.fromordinal(o) for o in range(start.toordinal(), end.toordinal() + 1))]
    location = {"daily": {"time": days, "precipitation_sum": [rain] * len(days), "temperature_2m_mean": [temp] * len(days)}}
    return [location] * (len(feed.STATES) if states is None else states)


class FakeSession:
    def __init__(self, bodies: list[object], headers: dict[str, str] | None = None) -> None:
        self.bodies = bodies
        self.asked: list[dict] = []
        self.headers = headers or {}

    def get(self, url: str, params: dict, timeout: tuple[int, int]) -> "FakeSession":
        self.asked.append(params)
        body = self.bodies[len(self.asked) - 1]
        if isinstance(body, Exception):
            raise body
        self.body = body
        return self

    def raise_for_status(self) -> None:
        pass

    def json(self) -> object:
        return self.body


def test_ranges_are_cut_to_a_year() -> None:
    chunks = feed.date_chunks(date(2016, 9, 1), date(2019, 12, 31))
    assert chunks[0] == (date(2016, 9, 1), date(2017, 9, 1))
    assert chunks[-1][1] == date(2019, 12, 31)
    assert all((b - a).days < 366 for a, b in chunks)
    assert feed.date_chunks(date(2018, 1, 2), date(2018, 1, 1)) == []


def test_rows_are_one_per_state_and_day() -> None:
    start, end = date(2018, 7, 1), date(2018, 7, 3)
    rows = feed.fetch_range(start, end, FakeSession([answer(start, end)]))  # type: ignore[arg-type]
    assert len(rows) == 27 * 3
    assert rows[0] == {"state": "AC", "weather_date": "2018-07-01", "precipitation_mm": "1.00", "temperature_c": "20.0"}
    assert {r["state"] for r in rows} == {s for s, _, _ in feed.STATES}


def test_one_request_carries_every_location() -> None:
    session = FakeSession([answer(date(2018, 7, 1), date(2018, 7, 1))])
    feed.fetch_range(date(2018, 7, 1), date(2018, 7, 1), session)  # type: ignore[arg-type]
    assert len(session.asked[0]["latitude"].split(",")) == 27 == len(session.asked[0]["longitude"].split(","))


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ({"error": True, "reason": "limit"}, "expected a list of 27"),
        (answer(date(2018, 7, 1), date(2018, 7, 1), states=26), "expected a list of 27"),
        (answer(date(2018, 7, 2), date(2018, 7, 2)), "days 2018-07-01..2018-07-01 expected"),
        (answer(date(2018, 7, 1), date(2018, 7, 1), rain=-1), "implausible"),
        (answer(date(2018, 7, 1), date(2018, 7, 1), rain=5000), "implausible"),
        (answer(date(2018, 7, 1), date(2018, 7, 1), temp=99), "implausible"),
        (answer(date(2018, 7, 1), date(2018, 7, 1), temp=None), "missing value"),  # type: ignore[arg-type]
    ],
)
def test_a_bad_answer_fails_the_range(body: object, message: str) -> None:
    with pytest.raises(feed.WeatherError, match=message):
        feed.fetch_range(date(2018, 7, 1), date(2018, 7, 1), FakeSession([body]))  # type: ignore[arg-type]


def test_a_down_feed_names_the_range() -> None:
    with pytest.raises(feed.WeatherError, match=r"weather 2018-07-01\.\.2018-07-01: ConnectionError"):
        feed.fetch_range(date(2018, 7, 1), date(2018, 7, 1), FakeSession([requests.ConnectionError("boom")]))  # type: ignore[arg-type]


def test_waits_for_the_window_when_the_budget_is_short(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(feed.time, "time", lambda: 1000.0)
    assert feed.wait_for_budget({"X-RateLimit-Remaining": "573", "X-RateLimit-Reset": "1030"}, 141) == 0  # type: ignore[arg-type]
    assert feed.wait_for_budget({"X-RateLimit-Remaining": "100", "X-RateLimit-Reset": "1030"}, 141) == 31  # type: ignore[arg-type]
    assert feed.wait_for_budget({}, 141) == 0  # type: ignore[arg-type]


def test_cost_matches_the_measured_values() -> None:
    assert feed.request_cost(31) == 27  # 27.0 measured
    assert round(feed.request_cost(365), 1) == 140.8  # 140.79 measured

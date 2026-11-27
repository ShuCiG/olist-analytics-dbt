"""The REST feeds (holidays, weather, FX rates) from fixtures instead of the network (#356).

Every scenario runs the real loader, which also asks three public APIs. From a shared CI
runner IP a rate limit or an outage there would fail a scenario that has nothing to do with
feeds. The fixtures keep the feeds' row shapes and the loader's own load code (range logic,
merge, dedup); only the HTTP call is replaced. The feeds' own checks have unit tests in
tests/ (answers cut off, implausible values, fallback source).

A feed outage scenario (#375) keeps the feed's real code and replaces only the HTTP session: the
named hosts refuse the connection or answer 429, the others answer from the fixtures. So the
fallback source, the error text and the all-or-nothing write are the loader's own.
"""

import re
from datetime import date, timedelta
from typing import Any
from urllib.parse import urlparse

import requests

# Fixed national holidays (month, day): nine, above the feed's minimum of eight per year
HOLIDAYS = ((1, 1), (4, 21), (5, 1), (9, 7), (10, 12), (11, 2), (11, 15), (11, 20), (12, 25))


def holiday_rows(years: list[int]) -> list[dict[str, str]]:
    return [
        {
            "holiday_date": date(year, month, day).isoformat(),
            "holiday_name": f"Holiday {month}-{day}",
            "holiday_type": "national",
            "holiday_year": str(year),
            "source": "brasilapi",
        }
        for year in years
        for month, day in HOLIDAYS
    ]


def days(start: date, end: date) -> list[date]:
    return [start + timedelta(days=n) for n in range((end - start).days + 1)]


def weather_rows(start: date, end: date, states: list[str]) -> list[dict[str, str]]:
    """Every state, every day; mild, repeatable values (some rain every third day)."""
    return [
        {
            "state": state,
            "weather_date": day.isoformat(),
            "precipitation_mm": f"{(day.toordinal() + i) % 3 * 4.0:.2f}",
            "temperature_c": f"{20 + (day.toordinal() + i) % 8:.1f}",
        }
        for i, state in enumerate(states)
        for day in days(start, end)
    ]


def fx_rows(start: date, end: date) -> list[dict[str, str]]:
    """Business days only, like PTAX; one rate per day."""
    return [
        {"rate_date": day.isoformat(), "brl_per_usd": f"{4 + day.toordinal() % 10 / 10:.6f}", "source": "bcb-ptax"}
        for day in days(start, end)
        if day.weekday() < 5
    ]


# Outage name -> {host: how it fails}. A host not listed answers like Nager.Date, from the fixtures
DOWN, RATE_LIMITED = "refuses the connection", "answers 429"
OUTAGES: dict[str, dict[str, str]] = {
    "holidays_primary": {"brasilapi.com.br": DOWN},
    "holidays_all": {"brasilapi.com.br": DOWN, "date.nager.at": RATE_LIMITED},
    "weather": {"archive-api.open-meteo.com": RATE_LIMITED},
    "fx_all": {"olinda.bcb.gov.br": DOWN, "api.frankfurter.dev": RATE_LIMITED},
}


class Answer:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code, self.body, self.headers = status, body, {}

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError(f"{self.status_code} Client Error: Too Many Requests for url")

    def json(self) -> Any:
        return self.body


class OutageSession:
    """Stands in for the feeds' requests session (the retries live in its adapter, not replayed here)."""

    def __init__(self, down: dict[str, str]) -> None:
        self.down = down

    def get(self, url: str, params: Any = None, timeout: Any = None) -> Answer:
        host = urlparse(url).netloc
        failure = self.down.get(host)
        if failure == DOWN:
            raise requests.ConnectionError(f"{host}: connection refused")
        if failure == RATE_LIMITED:
            return Answer(429, None)
        found = re.search(r"/(\d{4})(?:/|$)", url)
        assert found, url
        year = int(found.group(1))
        return Answer(
            200,
            [{"date": r["holiday_date"], "localName": r["holiday_name"], "global": True} for r in holiday_rows([year])],
        )


def install(holidays_feed: Any, weather_feed: Any, fx_feed: Any, outage: str = "") -> None:
    """Fixtures for every feed; the one named by `outage` keeps its own code and meets the failure."""
    states = [state for state, _, _ in weather_feed.STATES]
    session = OutageSession(OUTAGES[outage]) if outage else None
    if outage.startswith("holidays"):
        holidays_feed.make_session = lambda: session
    else:
        holidays_feed.fetch_years = holiday_rows
    if outage == "weather":
        weather_feed.make_session = lambda: session
    else:
        weather_feed.fetch_range = lambda start, end, session=None: weather_rows(start, end, states)
    if outage == "fx_all":
        fx_feed.make_session = lambda: session
    else:
        fx_feed.fetch_range = lambda start, end, session=None: fx_rows(start, end)

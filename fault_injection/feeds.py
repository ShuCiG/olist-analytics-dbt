"""The REST feeds (holidays, weather, FX rates) from fixtures instead of the network (#356).

Every scenario runs the real loader, which also asks three public APIs. From a shared CI
runner IP a rate limit or an outage there would fail a scenario that has nothing to do with
feeds. The fixtures keep the feeds' row shapes and the loader's own load code (range logic,
merge, dedup); only the HTTP call is replaced. The feeds' own checks have unit tests in
tests/ (answers cut off, implausible values, fallback source).
"""

from datetime import date, timedelta
from typing import Any

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


def install(holidays_feed: Any, weather_feed: Any, fx_feed: Any) -> None:
    states = [state for state, _, _ in weather_feed.STATES]
    holidays_feed.fetch_years = holiday_rows
    weather_feed.fetch_range = lambda start, end, session=None: weather_rows(start, end, states)
    fx_feed.fetch_range = lambda start, end, session=None: fx_rows(start, end)

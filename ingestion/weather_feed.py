"""Daily weather per Brazilian state from Open-Meteo (#349): the second external REST feed.

One point per state (its capital), daily precipitation sum and mean temperature, from the
archive API (ERA5 reanalysis, no key). Several coordinates go into one request; the cost of
a request is locations x days / 70 against 600 a minute (X-RateLimit headers), so the
loader asks for at most one year per request and waits for the window to reset when the
budget left is smaller than the next request.

Same rules as holidays_feed.py: timeout, retries with backoff, every answer checked before
anything is written (a changed or cut-off answer fails the run), all ranges fetched before the
first write. No fallback source: the weather is context, not a key of the marts; a down feed
fails the ingest and the next run asks again.

No Airflow or dlt imports: `ingestion/pipeline.py` loads what this returns.
"""

import math
import re
import time
from collections.abc import Mapping
from datetime import date, timedelta

import requests

try:  # imported as ingestion.weather_feed (tests) or run as a script from ingestion/
    from .holidays_feed import make_session
except ImportError:
    from holidays_feed import make_session  # type: ignore[no-redef]

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
TIMEOUT = (5, 60)  # connect, read (seconds)
MAX_DAYS_PER_REQUEST = 366
ARCHIVE_LAG_DAYS = 7  # the archive is final a few days late; newer days are not asked for
COST_DAYS = 70  # request cost = locations x days / 70 (measured, X-RateLimit-Cost)
MAX_WAIT = 65.0
DATE_FORMAT = re.compile(r"\d{4}-\d{2}-\d{2}")

# State capitals: (state, latitude, longitude). One point stands for the state.
STATES: tuple[tuple[str, float, float], ...] = (
    ("AC", -9.97, -67.81), ("AL", -9.67, -35.74), ("AM", -3.12, -60.02), ("AP", 0.03, -51.07),
    ("BA", -12.97, -38.51), ("CE", -3.73, -38.52), ("DF", -15.79, -47.88), ("ES", -20.32, -40.34),
    ("GO", -16.68, -49.25), ("MA", -2.53, -44.30), ("MG", -19.92, -43.94), ("MS", -20.47, -54.62),
    ("MT", -15.60, -56.10), ("PA", -1.46, -48.50), ("PB", -7.12, -34.86), ("PE", -8.05, -34.88),
    ("PI", -5.09, -42.80), ("PR", -25.43, -49.27), ("RJ", -22.91, -43.17), ("RN", -5.79, -35.21),
    ("RO", -8.76, -63.90), ("RR", 2.82, -60.67), ("RS", -30.03, -51.23), ("SC", -27.60, -48.55),
    ("SE", -10.91, -37.07), ("SP", -23.55, -46.63), ("TO", -10.18, -48.33),
)
MAX_PRECIPITATION_MM = 1000.0  # a day above this is not weather
TEMPERATURE_RANGE = (-10.0, 45.0)  # daily mean, Brazil


class WeatherError(RuntimeError):
    """The feed did not give usable weather for a date range."""


def last_available_day(today: date) -> date:
    return today - timedelta(days=ARCHIVE_LAG_DAYS)


def date_chunks(start: date, end: date) -> list[tuple[date, date]]:
    """Ranges of at most a year, first to last day inclusive."""
    chunks = []
    while start <= end:
        stop = min(end, start + timedelta(days=MAX_DAYS_PER_REQUEST - 1))
        chunks.append((start, stop))
        start = stop + timedelta(days=1)
    return chunks


def request_cost(days: int) -> float:
    return len(STATES) * max(1.0, days / COST_DAYS)


def wait_for_budget(headers: Mapping[str, str], next_cost: float) -> float:
    """Seconds to sleep before the next request so it fits the minute's budget (0 when it does)."""
    try:
        remaining = float(headers["X-RateLimit-Remaining"])
        reset = float(headers["X-RateLimit-Reset"])
    except (KeyError, ValueError):
        return 0.0
    if remaining >= next_cost:
        return 0.0
    return min(MAX_WAIT, max(1.0, reset - time.time() + 1.0))


def check_chunk(body: object, start: date, end: date) -> list[dict[str, str]]:
    """Rows of one answer: one per state and day, every value checked; raises on a bad answer."""
    if not isinstance(body, list) or len(body) != len(STATES):
        got = len(body) if isinstance(body, list) else type(body).__name__
        raise WeatherError(f"expected a list of {len(STATES)} locations, got {got}")
    days = [(start + timedelta(days=i)).isoformat() for i in range((end - start).days + 1)]
    rows: list[dict[str, str]] = []
    for (state, _, _), location in zip(STATES, body, strict=True):
        daily = location.get("daily") if isinstance(location, dict) else None
        if not isinstance(daily, dict):
            raise WeatherError(f"{state}: no daily block")
        if daily.get("time") != days:
            raise WeatherError(f"{state}: days {start}..{end} expected, got {len(daily.get('time') or [])} days")
        for day, rain, temp in zip(days, daily["precipitation_sum"], daily["temperature_2m_mean"], strict=True):
            if not DATE_FORMAT.fullmatch(day):
                raise WeatherError(f"{state}: bad date {day!r}")
            if rain is None or temp is None:
                raise WeatherError(f"{state} {day}: missing value (precipitation {rain}, temperature {temp})")
            if not 0 <= rain <= MAX_PRECIPITATION_MM or not TEMPERATURE_RANGE[0] <= temp <= TEMPERATURE_RANGE[1]:
                raise WeatherError(f"{state} {day}: implausible value (precipitation {rain} mm, temperature {temp})")
            rows.append(
                {"state": state, "weather_date": day, "precipitation_mm": f"{rain:.2f}", "temperature_c": f"{temp:.1f}"}
            )
    return rows


def fetch_range(start: date, end: date, session: requests.Session | None = None) -> list[dict[str, str]]:
    """Weather of every state for every day from start to end (inclusive), or an error."""
    session = session or make_session()
    rows: list[dict[str, str]] = []
    wait = 0.0
    for first, last in date_chunks(start, end):
        cost = request_cost((last - first).days + 1)
        if wait:
            print(f"Weather: waiting {wait:.0f} s for the rate limit")
            time.sleep(wait)
        params = {
            "latitude": ",".join(str(lat) for _, lat, _ in STATES),
            "longitude": ",".join(str(lon) for _, _, lon in STATES),
            "start_date": first.isoformat(),
            "end_date": last.isoformat(),
            "daily": "precipitation_sum,temperature_2m_mean",
            "timezone": "auto",
        }
        try:
            response = session.get(ARCHIVE_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            rows += check_chunk(response.json(), first, last)
        except (requests.RequestException, ValueError, KeyError, TypeError, WeatherError) as error:
            raise WeatherError(f"weather {first}..{last}: {type(error).__name__}: {error}") from error
        wait = wait_for_budget(response.headers, math.ceil(cost) + 1)
    return rows

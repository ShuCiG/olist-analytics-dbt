"""fault_injection/feeds.py: the fixture rows pass the feeds' own checks (#356), so a scenario
that loads them sees what a good answer of the real feed would give."""

from datetime import date
from types import SimpleNamespace

import pytest
import requests

from fault_injection import feeds
from ingestion import fx_feed, holidays_feed, weather_feed

START, END = date(2018, 1, 1), date(2018, 3, 31)


def test_holiday_fixtures_pass_the_year_check() -> None:
    rows = feeds.holiday_rows([2018, 2019])
    for year in (2018, 2019):
        of_year = [r for r in rows if r["holiday_year"] == str(year)]
        assert len(holidays_feed.check_year(of_year, year)) == len(feeds.HOLIDAYS)


def test_fx_fixtures_pass_the_rate_check_and_skip_weekends() -> None:
    rows = feeds.fx_rows(START, END)
    checked = fx_feed.check_rows([(r["rate_date"], float(r["brl_per_usd"])) for r in rows], START, END)
    assert len(checked) == len(rows) == 65
    assert all(date.fromisoformat(r["rate_date"]).weekday() < 5 for r in rows)


def test_weather_fixtures_cover_every_state_and_day_within_limits() -> None:
    states = [state for state, _, _ in weather_feed.STATES]
    rows = feeds.weather_rows(START, END, states)
    assert len(rows) == len(states) * 90
    assert {r["state"] for r in rows} == set(states)
    low, high = weather_feed.TEMPERATURE_RANGE
    assert all(0 <= float(r["precipitation_mm"]) <= weather_feed.MAX_PRECIPITATION_MM for r in rows)
    assert all(low <= float(r["temperature_c"]) <= high for r in rows)


def test_install_replaces_the_network_calls() -> None:
    holidays, weather, fx = SimpleNamespace(), SimpleNamespace(STATES=(("SP", 0.0, 0.0),)), SimpleNamespace()
    feeds.install(holidays, weather, fx)
    assert holidays.fetch_years([2018])[0]["source"] == "brasilapi"
    assert {r["state"] for r in weather.fetch_range(START, END)} == {"SP"}
    assert fx.fetch_range(START, END)[0]["source"] == "bcb-ptax"


def test_outage_session_refuses_rate_limits_or_answers_like_nager() -> None:
    session = feeds.OutageSession({"brasilapi.com.br": feeds.DOWN, "api.frankfurter.dev": feeds.RATE_LIMITED})
    with pytest.raises(requests.ConnectionError):
        session.get(holidays_feed.BRASILAPI_URL.format(year=2018))
    limited = session.get("https://api.frankfurter.dev/v1/2018-01-01..2018-01-31")
    with pytest.raises(requests.HTTPError):
        limited.raise_for_status()
    rows = holidays_feed.from_nager(session.get(holidays_feed.NAGER_URL.format(year=2018)).json())
    assert len(holidays_feed.check_year(rows, 2018)) == len(feeds.HOLIDAYS)


def test_holidays_outage_runs_the_real_fallback_and_the_real_error() -> None:
    holidays = SimpleNamespace(make_session=None)
    feeds.install(holidays, SimpleNamespace(STATES=()), SimpleNamespace(), outage="holidays_primary")
    session = holidays.make_session()
    rows = holidays_feed.fetch_year(2018, session)
    assert {r["source"] for r in rows} == {"nager"}
    session = feeds.OutageSession(feeds.OUTAGES["holidays_all"])
    with pytest.raises(holidays_feed.HolidaysError, match="brasilapi: ConnectionError.*nager: HTTPError"):
        holidays_feed.fetch_year(2018, session)  # type: ignore[arg-type]

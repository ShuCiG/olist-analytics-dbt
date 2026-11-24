"""Daily BRL per USD exchange rate from public REST APIs (#351): the third external feed, and the
first one that really changes every day.

Banco Central do Brasil PTAX (olinda OData API, `CotacaoDolarPeriodo`) is the primary source: the
official closing rate, the mean of buy and sell. Frankfurter (ECB reference rates) is the fallback,
asked for the same range only when the primary fails. No key, no auth. Banks publish on business
days only: weekends and Brazilian bank holidays have no row (dbt carries the last rate forward).

Same rules as holidays_feed.py: timeout, retries with backoff, every answer checked before anything
is written, the whole range fetched before the first write, rows tagged with their source. The two
sources do not give the same number (the ECB rate is a mid-day Europe fixing, PTAX a Brazilian
midday average: 0.3 % apart on average, a few % on bad days), so a range comes from one source.

No Airflow or dlt imports: `ingestion/pipeline.py` loads what this returns.
"""

import re
from collections.abc import Callable
from datetime import date, datetime

import requests

try:  # imported as ingestion.fx_feed (tests) or run as a script from ingestion/
    from .holidays_feed import make_session
except ImportError:
    from holidays_feed import make_session  # type: ignore[no-redef]

PTAX_URL = (
    "https://olinda.bcb.gov.br/olinda/servico/PTAX/versao/v1/odata/"
    "CotacaoDolarPeriodo(dataInicial=@i,dataFinalCotacao=@f)"
)
FRANKFURTER_URL = "https://api.frankfurter.dev/v1/{start}..{end}"
TIMEOUT = (5, 30)  # connect, read (seconds)
PTAX_MAX_ROWS = 10000  # $top; ten years are ~2,500 business days
# A real rate stays inside this band (BRL per USD: 1.5 in 2011, 6.3 at the 2020 low); outside is a
# broken answer (a swapped pair, a rate in cents), not a market move
RATE_RANGE = (1.5, 10.0)
DATE_FORMAT = re.compile(r"\d{4}-\d{2}-\d{2}")
# A range of at least this many days has a business day in it: an empty answer is a failed feed
MIN_DAYS_FOR_ROWS = 7


class FxError(RuntimeError):
    """The feed did not give usable rates for a date range (all sources tried)."""


def check_rows(rows: list[tuple[str, float]], start: date, end: date) -> list[dict[str, str]]:
    """One row per date inside the range, every rate checked; raises on a bad answer.

    A date that appears twice (PTAX repeats a bulletin now and then) must carry the same rate.
    """
    by_date: dict[str, float] = {}
    for day, rate in rows:
        if not DATE_FORMAT.fullmatch(day):
            raise FxError(f"bad rate date {day!r}")
        if not start <= date.fromisoformat(day) <= end:
            raise FxError(f"rate date {day} is outside the asked range {start}..{end}")
        if not RATE_RANGE[0] <= rate <= RATE_RANGE[1]:
            raise FxError(f"{day}: implausible rate {rate} BRL per USD")
        if day in by_date and abs(by_date[day] - rate) > 1e-9:
            raise FxError(f"{day}: two different rates ({by_date[day]} and {rate})")
        by_date[day] = rate
    if not by_date and (end - start).days + 1 >= MIN_DAYS_FOR_ROWS:
        raise FxError(f"no rates for {start}..{end}")
    return [{"rate_date": day, "brl_per_usd": f"{by_date[day]:.6f}"} for day in sorted(by_date)]


def ptax_rows(session: requests.Session, start: date, end: date) -> list[tuple[str, float]]:
    # The API wants US-style dates in quotes: '09-01-2016'
    params = {
        "@i": f"'{start:%m-%d-%Y}'",
        "@f": f"'{end:%m-%d-%Y}'",
        "$top": str(PTAX_MAX_ROWS),
        "$format": "json",
    }
    response = session.get(PTAX_URL, params=params, timeout=TIMEOUT)
    response.raise_for_status()
    body = response.json()
    items = body.get("value") if isinstance(body, dict) else None
    if not isinstance(items, list):
        raise FxError("PTAX: no 'value' list in the answer")
    if len(items) >= PTAX_MAX_ROWS:
        raise FxError(f"PTAX: {len(items)} rows reach the {PTAX_MAX_ROWS} row limit, the answer may be cut off")
    rows = []
    for item in items:
        stamp = datetime.fromisoformat(str(item["dataHoraCotacao"]))
        rows.append((stamp.date().isoformat(), (float(item["cotacaoCompra"]) + float(item["cotacaoVenda"])) / 2))
    return rows


def frankfurter_rows(session: requests.Session, start: date, end: date) -> list[tuple[str, float]]:
    url = FRANKFURTER_URL.format(start=start.isoformat(), end=end.isoformat())
    response = session.get(url, params={"base": "USD", "symbols": "BRL"}, timeout=TIMEOUT)
    response.raise_for_status()
    body = response.json()
    rates = body.get("rates") if isinstance(body, dict) else None
    if not isinstance(rates, dict):
        raise FxError("Frankfurter: no 'rates' object in the answer")
    return [(str(day), float(value["BRL"])) for day, value in rates.items()]


SOURCES: tuple[tuple[str, Callable[[requests.Session, date, date], list[tuple[str, float]]]], ...] = (
    ("bcb-ptax", ptax_rows),
    ("frankfurter", frankfurter_rows),
)


def fetch_range(start: date, end: date, session: requests.Session | None = None) -> list[dict[str, str]]:
    """Rates from start to end (inclusive), each row tagged with its source; the fallback only when
    the first source fails. An empty list is a valid answer for a range without a business day."""
    session = session or make_session()
    problems: list[str] = []
    for name, rows_of in SOURCES:
        try:
            rows = check_rows(rows_of(session, start, end), start, end)
        except (requests.RequestException, ValueError, KeyError, TypeError, FxError) as error:
            problems.append(f"{name}: {type(error).__name__}: {error}")
            continue
        if problems:
            print(f"FX rates {start}..{end}: fallback source {name} used ({problems[0]})")
        return [{**row, "source": name} for row in rows]
    raise FxError(f"no rates for {start}..{end}: " + "; ".join(problems))

"""Brazilian national holidays from a public REST API (#348): the first external feed.

BrasilAPI is the primary source (`/api/feriados/v1/{year}`), Nager.Date the fallback
(`/api/v3/PublicHolidays/{year}/BR`). No key, no auth. Both answer with a JSON list; the
answer is checked before anything is written, so a changed or broken response fails the
run with a message instead of loading garbage.

The feed is treated as critical on purpose: it is the pattern for the next REST sources
(retries with backoff, a timeout, a fallback source, shape checks, coverage checks in dbt).

No Airflow or dlt imports: `ingestion/pipeline.py` loads what this returns.
"""

import re
from collections.abc import Callable
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BRASILAPI_URL = "https://brasilapi.com.br/api/feriados/v1/{year}"
NAGER_URL = "https://date.nager.at/api/v3/PublicHolidays/{year}/BR"
TIMEOUT = (5, 20)  # connect, read (seconds)
# Brazil has 10+ national holidays a year (Carnaval counts two days); fewer means a cut-off answer
MIN_HOLIDAYS_PER_YEAR = 8
NATIONAL = "national"
DATE_FORMAT = re.compile(r"\d{4}-\d{2}-\d{2}")


class HolidaysError(RuntimeError):
    """The feed did not give usable holidays for a year (all sources tried)."""


def make_session() -> requests.Session:
    """A session that retries 429 / 5xx and connection errors with backoff (1, 2, 4, 8 s)."""
    retry = Retry(
        total=4,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "olist-analytics-dbt (portfolio project)"
    return session


def get_json(session: requests.Session, url: str) -> list[dict]:
    response = session.get(url, timeout=TIMEOUT)
    response.raise_for_status()
    body = response.json()
    if not isinstance(body, list):
        raise HolidaysError(f"{url}: expected a JSON list, got {type(body).__name__}")
    return body


def from_brasilapi(body: list[dict]) -> list[dict[str, str]]:
    return [
        {"holiday_date": str(item["date"]), "holiday_name": str(item["name"]), "holiday_type": str(item["type"])}
        for item in body
    ]


def from_nager(body: list[dict]) -> list[dict[str, str]]:
    # `global` = a holiday for the whole country; regional ones (counties) are left out
    return [
        {"holiday_date": str(item["date"]), "holiday_name": str(item["localName"]), "holiday_type": NATIONAL}
        for item in body
        if item.get("global")
    ]


def check_year(rows: list[dict[str, str]], year: int) -> list[dict[str, str]]:
    """One row per date (names of two holidays on one day are joined); raises on a bad answer."""
    by_date: dict[str, dict[str, str]] = {}
    for row in rows:
        day = row["holiday_date"]
        if not DATE_FORMAT.fullmatch(day) or int(day[:4]) != year:
            raise HolidaysError(f"holiday date {day!r} is not a date of {year}")
        if not row["holiday_name"].strip():
            raise HolidaysError(f"holiday {day} has no name")
        if day in by_date:
            if row["holiday_name"] not in by_date[day]["holiday_name"].split(" / "):
                by_date[day]["holiday_name"] += f" / {row['holiday_name']}"
        else:
            by_date[day] = dict(row)
    if len(by_date) < MIN_HOLIDAYS_PER_YEAR:
        raise HolidaysError(f"only {len(by_date)} holidays for {year}, expected at least {MIN_HOLIDAYS_PER_YEAR}")
    for row in by_date.values():
        date.fromisoformat(row["holiday_date"])  # a well-formed but impossible date (2018-02-30)
    return sorted(by_date.values(), key=lambda r: r["holiday_date"])


SOURCES: tuple[tuple[str, str, Callable[[list[dict]], list[dict[str, str]]]], ...] = (
    ("brasilapi", BRASILAPI_URL, from_brasilapi),
    ("nager", NAGER_URL, from_nager),
)


def fetch_year(year: int, session: requests.Session | None = None) -> list[dict[str, str]]:
    """Holidays of one year, each row tagged with its source; the fallback only when the first fails."""
    session = session or make_session()
    problems: list[str] = []
    for name, url, convert in SOURCES:
        try:
            rows = check_year(convert(get_json(session, url.format(year=year))), year)
        except (requests.RequestException, ValueError, KeyError, TypeError, HolidaysError) as error:
            problems.append(f"{name}: {type(error).__name__}: {error}")
            continue
        if problems:
            print(f"Holidays {year}: fallback source {name} used ({problems[0]})")
        return [{**row, "holiday_year": str(year), "source": name} for row in rows]
    raise HolidaysError(f"no holidays for {year}: " + "; ".join(problems))


def fetch_years(years: list[int]) -> list[dict[str, str]]:
    """Every year or an error: nothing partial reaches the warehouse."""
    session = make_session()
    return [row for year in years for row in fetch_year(year, session)]

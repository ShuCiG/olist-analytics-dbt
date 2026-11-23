"""ingestion/holidays_feed.py: the shape checks and the fallback, on recorded answers (no network)."""

import pytest
import requests

from ingestion import holidays_feed as feed


def brasilapi(year: int, n: int = 12) -> list[dict]:
    return [{"date": f"{year}-{m:02d}-05", "name": f"Dia {m}", "type": "national", "weekday": "x"} for m in range(1, n + 1)]


def nager(year: int, n: int = 12) -> list[dict]:
    return [
        {"date": f"{year}-{m:02d}-05", "localName": f"Dia {m}", "name": f"Day {m}", "global": m != 12, "counties": None}
        for m in range(1, n + 1)
    ]


class FakeSession:
    """Answers by URL; a value that is an exception is raised."""

    def __init__(self, answers: dict[str, object]) -> None:
        self.answers = answers
        self.asked: list[str] = []

    def get(self, url: str, timeout: tuple[int, int]) -> "FakeSession":
        self.asked.append(url)
        answer = self.answers[url.split("/")[2]]
        if isinstance(answer, Exception):
            raise answer
        self.body = answer
        return self

    def raise_for_status(self) -> None:
        pass

    def json(self) -> object:
        return self.body


def session(brasil: object, naeger: object = None) -> requests.Session:
    return FakeSession({"brasilapi.com.br": brasil, "date.nager.at": naeger if naeger is not None else nager(2018)})  # type: ignore[return-value]


def test_primary_source_rows_are_tagged_and_sorted() -> None:
    rows = feed.fetch_year(2018, session(list(reversed(brasilapi(2018)))))
    assert len(rows) == 12
    assert rows[0] == {
        "holiday_date": "2018-01-05", "holiday_name": "Dia 1", "holiday_type": "national", "holiday_year": "2018", "source": "brasilapi"
    }
    assert [r["holiday_date"] for r in rows] == sorted(r["holiday_date"] for r in rows)


def test_fallback_when_the_primary_is_down(capsys: pytest.CaptureFixture[str]) -> None:
    rows = feed.fetch_year(2018, session(requests.ConnectionError("boom")))
    assert {r["source"] for r in rows} == {"nager"}
    assert len(rows) == 11  # the regional (not global) holiday is left out
    assert "fallback source nager used" in capsys.readouterr().out


@pytest.mark.parametrize(
    "bad",
    [
        {"message": "rate limited"},  # not a list
        brasilapi(2018, 3),  # cut-off answer
        brasilapi(2019),  # wrong year
        [{"date": "2018-02-30", "name": "x", "type": "national"}] * 1 + brasilapi(2018)[:9],  # impossible date
        [{"name": "no date"}],  # missing key
        [{"date": "2018-01-01", "name": " ", "type": "national"}] + brasilapi(2018)[1:],  # no name
    ],
)
def test_bad_answer_from_both_sources_raises(bad: object) -> None:
    with pytest.raises(feed.HolidaysError, match="no holidays for 2018: brasilapi: .*; nager: "):
        feed.fetch_year(2018, session(bad, requests.Timeout("slow")))


def test_bad_primary_answer_falls_back() -> None:
    rows = feed.fetch_year(2018, session({"message": "oops"}))
    assert {r["source"] for r in rows} == {"nager"}


def test_two_holidays_on_one_day_are_joined() -> None:
    body = brasilapi(2018) + [{"date": "2018-01-05", "name": "Outro", "type": "national"}]
    rows = feed.fetch_year(2018, session(body))
    assert len(rows) == 12
    assert rows[0]["holiday_name"] == "Dia 1 / Outro"


def test_fetch_years_is_all_or_nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[int] = []

    def fake(year: int, _session: object) -> list[dict[str, str]]:
        calls.append(year)
        if year == 2019:
            raise feed.HolidaysError("no holidays for 2019")
        return [{"holiday_year": str(year)}]

    monkeypatch.setattr(feed, "fetch_year", fake)
    monkeypatch.setattr(feed, "make_session", lambda: object())
    with pytest.raises(feed.HolidaysError):
        feed.fetch_years([2018, 2019, 2020])
    assert calls == [2018, 2019]


def test_session_retries_server_errors_with_backoff() -> None:
    adapter = feed.make_session().get_adapter("https://brasilapi.com.br")
    retry = adapter.max_retries  # type: ignore[attr-defined]
    assert retry.total == 4
    assert 503 in retry.status_forcelist and 429 in retry.status_forcelist

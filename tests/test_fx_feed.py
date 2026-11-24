"""ingestion/fx_feed.py: both sources on recorded answers, the checks and the fallback."""

from datetime import date

import pytest
import requests

from ingestion import fx_feed as feed

START, END = date(2018, 7, 2), date(2018, 7, 6)


def ptax(*items: tuple[str, float, float]) -> dict:
    return {"value": [{"cotacaoCompra": b, "cotacaoVenda": s, "dataHoraCotacao": f"{d} 13:06:30.4"} for d, b, s in items]}


def frankfurter(**rates: float) -> dict:
    return {"rates": {d.replace("_", "-"): {"BRL": r} for d, r in rates.items()}}


class FakeSession:
    """Answers per URL part: a body, or an exception to raise."""

    def __init__(self, ptax_body: object, frankfurter_body: object = None) -> None:
        self.bodies = {"olinda": ptax_body, "frankfurter": frankfurter_body}
        self.asked: list[tuple[str, dict]] = []

    def get(self, url: str, params: dict, timeout: tuple[int, int]) -> "FakeSession":
        self.asked.append((url, params))
        body = self.bodies["olinda" if "olinda" in url else "frankfurter"]
        if isinstance(body, Exception):
            raise body
        self.body = body
        return self

    def raise_for_status(self) -> None:
        pass

    def json(self) -> object:
        return self.body


GOOD_PTAX = ptax(("2018-07-02", 3.8, 3.82), ("2018-07-03", 3.84, 3.86))


def fetch(session: FakeSession) -> list[dict[str, str]]:
    return feed.fetch_range(START, END, session)  # type: ignore[arg-type]


def test_ptax_rate_is_the_mean_of_buy_and_sell() -> None:
    rows = fetch(FakeSession(GOOD_PTAX))
    assert rows == [
        {"rate_date": "2018-07-02", "brl_per_usd": "3.810000", "source": "bcb-ptax"},
        {"rate_date": "2018-07-03", "brl_per_usd": "3.850000", "source": "bcb-ptax"},
    ]


def test_ptax_asks_with_quoted_us_dates() -> None:
    session = FakeSession(GOOD_PTAX)
    fetch(session)
    assert session.asked[0][1]["@i"] == "'07-02-2018'" and session.asked[0][1]["@f"] == "'07-06-2018'"


def test_the_same_bulletin_twice_is_one_row() -> None:
    rows = fetch(FakeSession(ptax(("2018-07-02", 3.8, 3.82), ("2018-07-02", 3.8, 3.82))))
    assert len(rows) == 1


def test_two_rates_for_one_day_fail() -> None:
    with pytest.raises(feed.FxError, match="two different rates"):
        feed.check_rows([("2018-07-02", 3.8), ("2018-07-02", 3.9)], START, END)


@pytest.mark.parametrize("rate", [0.0, 0.38, 38.0, -3.8])
def test_implausible_rate_fails(rate: float) -> None:
    with pytest.raises(feed.FxError, match="implausible"):
        feed.check_rows([("2018-07-02", rate)], START, END)


def test_date_outside_the_range_fails() -> None:
    with pytest.raises(feed.FxError, match="outside"):
        feed.check_rows([("2018-07-09", 3.8)], START, END)


def test_empty_answer_is_fine_for_a_weekend_and_a_failure_for_a_week() -> None:
    assert feed.check_rows([], date(2018, 7, 7), date(2018, 7, 8)) == []
    with pytest.raises(feed.FxError, match="no rates"):
        feed.check_rows([], date(2018, 7, 2), date(2018, 7, 8))


def test_fallback_when_the_primary_is_down_tags_the_source() -> None:
    session = FakeSession(requests.ConnectionError("down"), frankfurter(**{"2018_07_02": 3.79}))
    assert fetch(session) == [{"rate_date": "2018-07-02", "brl_per_usd": "3.790000", "source": "frankfurter"}]
    assert session.asked[1][1] == {"base": "USD", "symbols": "BRL"}


def test_fallback_when_the_primary_answer_is_broken() -> None:
    session = FakeSession({"unexpected": 1}, frankfurter(**{"2018_07_02": 3.79}))
    assert fetch(session)[0]["source"] == "frankfurter"


def test_cut_off_ptax_answer_fails_over() -> None:
    many = ptax(*[("2018-07-02", 3.8, 3.82)] * feed.PTAX_MAX_ROWS)
    assert fetch(FakeSession(many, frankfurter(**{"2018_07_02": 3.79})))[0]["source"] == "frankfurter"


def test_both_down_fails_with_both_reasons() -> None:
    session = FakeSession(requests.ConnectionError("a"), requests.Timeout("b"))
    with pytest.raises(feed.FxError) as error:
        fetch(session)
    assert "bcb-ptax: ConnectionError" in str(error.value) and "frankfurter: Timeout" in str(error.value)

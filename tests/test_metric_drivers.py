"""Metric change drivers (D110): the maths and the email block, on a fake runner (no warehouse, no MetricFlow)."""

import datetime
from pathlib import Path
from typing import Any

import pytest

from alerting import airflow_emails, metric_drivers
from alerting.metric_drivers import MetricSpec, NotDecomposable, Period

BEFORE = Period(datetime.date(2017, 10, 1), datetime.date(2017, 10, 31))
AFTER = Period(datetime.date(2017, 11, 1), datetime.date(2017, 11, 30))
DIM = "customer__state"


def manifest(*metrics: dict[str, Any]) -> dict[str, Any]:
    return {"metrics": list(metrics)}


def simple(name: str, agg: str) -> dict[str, Any]:
    return {"name": name, "type": "simple", "type_params": {"metric_aggregation_params": {"agg": agg}}}


def ratio(name: str, numerator: str, denominator: str) -> dict[str, Any]:
    return {"name": name, "type": "ratio", "type_params": {"numerator": {"name": numerator}, "denominator": {"name": denominator}}}


SPECS = metric_drivers.specs_from_manifest(
    manifest(
        simple("revenue", "sum"),
        simple("orders", "count"),
        simple("customers", "count_distinct"),
        simple("avg_score", "average"),
        simple("late", "count"),
        simple("delivered", "count"),
        ratio("late_rate", "late", "delivered"),
        ratio("revenue_per_customer", "revenue", "customers"),
        {"name": "growth", "type": "derived", "type_params": {}},
    )
)


def fake_runner(tables: dict[datetime.date, list[dict[str, Any]]], dimension: str = DIM, unassigned: dict[str, float] | None = None) -> metric_drivers.Runner:
    """Rows per member for each period; no group by = their sum (plus `unassigned`, rows the dimension doesn't cover)."""

    def run(metrics: list[str], group_by: list[str], start: datetime.date, end: datetime.date) -> list[dict[str, Any]]:
        rows = tables[start]
        if group_by:
            assert group_by == [dimension]
            return [{dimension: r[dimension], **{m: r.get(m) for m in metrics}} for r in rows]
        return [{m: sum(r.get(m) or 0 for r in rows) + (unassigned or {}).get(m, 0) for m in metrics}]

    return run


def states(**members: float) -> list[dict[str, Any]]:
    return [{DIM: name, "revenue": value} for name, value in members.items()]


# --- which metrics can be split ------------------------------------------------------------------


def test_sums_counts_and_ratios_of_them_can_be_split() -> None:
    assert SPECS["revenue"] == MetricSpec("revenue", "additive")
    assert SPECS["orders"] == MetricSpec("orders", "additive")
    assert SPECS["late_rate"] == MetricSpec("late_rate", "ratio", "late", "delivered")


@pytest.mark.parametrize("name", ["customers", "avg_score", "revenue_per_customer", "growth"])
def test_distinct_counts_averages_and_what_is_built_on_them_are_refused(name: str) -> None:
    with pytest.raises(NotDecomposable, match="cannot be split"):
        metric_drivers.spec_for(SPECS, name)


def test_unknown_metric_lists_the_known_ones() -> None:
    with pytest.raises(ValueError, match="Unknown metric 'profit'.*revenue"):
        metric_drivers.spec_for(SPECS, "profit")


def test_refusal_names_the_reason() -> None:
    with pytest.raises(NotDecomposable, match="count_distinct"):
        metric_drivers.spec_for(SPECS, "customers")
    with pytest.raises(NotDecomposable, match="customers: aggregation 'count_distinct'"):
        metric_drivers.spec_for(SPECS, "revenue_per_customer")


# --- additive metrics ----------------------------------------------------------------------------


def test_additive_drivers_are_ranked_and_add_up_to_the_total() -> None:
    runner = fake_runner(
        {
            BEFORE.start: states(SP=100, RJ=50, MG=40, BA=10, GO=5),
            AFTER.start: states(SP=160, RJ=45, MG=80, BA=10, GO=0),
        }
    )
    result = metric_drivers.explain(runner, SPECS, "revenue", BEFORE, AFTER, [DIM], top=2)

    assert (result.before, result.after, result.delta) == (205, 295, 90)
    assert result.relative == pytest.approx(90 / 205)
    (breakdown,) = result.breakdowns
    assert [(d.member, d.delta) for d in breakdown.drivers] == [("SP", 60), ("MG", 40)]
    assert breakdown.other is not None
    assert breakdown.other.member == "other"
    assert breakdown.other.delta == pytest.approx(-10)  # RJ -5, BA 0, GO -5
    assert sum(d.delta for d in breakdown.drivers) + breakdown.other.delta == pytest.approx(result.delta)
    assert [d.share for d in breakdown.drivers] == pytest.approx([60 / 90, 40 / 90])
    assert breakdown.members == 5
    assert breakdown.explained == pytest.approx(100 / 90)  # the top two overshoot: others fell


def test_a_member_only_in_one_period_counts_from_zero() -> None:
    runner = fake_runner({BEFORE.start: states(SP=100, RJ=50), AFTER.start: states(SP=100, AC=30)})
    (breakdown,) = metric_drivers.explain(runner, SPECS, "revenue", BEFORE, AFTER, [DIM]).breakdowns
    by_member = {d.member: d.delta for d in breakdown.drivers}
    assert by_member == {"RJ": -50, "AC": 30, "SP": 0}
    assert breakdown.other is None  # everything is listed and adds up


def test_what_the_dimension_does_not_cover_goes_to_other() -> None:
    # 20 of revenue belongs to orders without a state; the total still adds up
    runner = fake_runner({BEFORE.start: states(SP=100), AFTER.start: states(SP=110)}, unassigned={"revenue": 20})
    (breakdown,) = metric_drivers.explain(runner, SPECS, "revenue", BEFORE, AFTER, [DIM]).breakdowns
    # unassigned is in both periods, so the total change is SP's 10
    assert breakdown.other is None
    assert breakdown.drivers[0].delta == 10


def test_missing_values_are_zero() -> None:
    runner = fake_runner({BEFORE.start: [{DIM: None, "revenue": None}], AFTER.start: states(SP=5)})
    (breakdown,) = metric_drivers.explain(runner, SPECS, "revenue", BEFORE, AFTER, [DIM]).breakdowns
    assert {d.member: d.delta for d in breakdown.drivers} == {"SP": 5, "(none)": 0}


def test_no_change_gives_no_shares() -> None:
    runner = fake_runner({BEFORE.start: states(SP=5), AFTER.start: states(SP=5)})
    result = metric_drivers.explain(runner, SPECS, "revenue", BEFORE, AFTER, [DIM])
    assert result.delta == 0
    assert result.breakdowns[0].drivers[0].share is None
    assert result.breakdowns[0].explained == 0


# --- ratio metrics -------------------------------------------------------------------------------


def rates(**members: tuple[float, float]) -> list[dict[str, Any]]:
    return [{DIM: name, "late": late, "delivered": delivered} for name, (late, delivered) in members.items()]


def test_ratio_change_splits_into_mix_and_rate_that_add_up() -> None:
    # A: 10/100 -> 30/200, B: 40/100 -> 40/100. Total 25% -> 23.33%
    runner = fake_runner({BEFORE.start: rates(A=(10, 100), B=(40, 100)), AFTER.start: rates(A=(30, 200), B=(40, 100))})
    result = metric_drivers.explain(runner, SPECS, "late_rate", BEFORE, AFTER, [DIM])

    assert result.kind == "ratio"
    assert result.before == pytest.approx(0.25)
    assert result.after == pytest.approx(70 / 300)
    assert result.delta == pytest.approx(-1 / 60)
    (breakdown,) = result.breakdowns
    by_member = {d.member: d for d in breakdown.drivers}
    # A: its rate rose 5 points on two thirds of the volume; it also became a bigger share of volume,
    # and A's rate is below the average, so the mix pulls the total down. B only lost share.
    assert by_member["A"].rate == pytest.approx(2 / 3 * 0.05)
    assert by_member["A"].mix == pytest.approx(-0.025)
    assert by_member["B"].rate == pytest.approx(0)
    assert by_member["B"].mix == pytest.approx(-0.025)
    assert sum(d.mix + d.rate for d in breakdown.drivers if d.mix is not None and d.rate is not None) == pytest.approx(result.delta)
    assert all(d.delta == pytest.approx((d.mix or 0) + (d.rate or 0)) for d in breakdown.drivers)
    assert breakdown.other is None


def test_ratio_with_only_a_mix_shift_has_no_rate_effect() -> None:
    # every member keeps its rate; volume moves to the cheaper member
    runner = fake_runner({BEFORE.start: rates(A=(10, 100), B=(40, 100)), AFTER.start: rates(A=(30, 300), B=(40, 100))})
    result = metric_drivers.explain(runner, SPECS, "late_rate", BEFORE, AFTER, [DIM])
    (breakdown,) = result.breakdowns
    assert all(d.rate == pytest.approx(0) for d in breakdown.drivers)
    assert sum(d.mix or 0 for d in breakdown.drivers) == pytest.approx(result.delta)


def test_ratio_new_and_vanished_members_are_pure_mix() -> None:
    runner = fake_runner({BEFORE.start: rates(A=(10, 100), OLD=(40, 100)), AFTER.start: rates(A=(10, 100), NEW=(60, 100))})
    result = metric_drivers.explain(runner, SPECS, "late_rate", BEFORE, AFTER, [DIM])
    (breakdown,) = result.breakdowns
    by_member = {d.member: d for d in breakdown.drivers}
    assert by_member["NEW"].rate == pytest.approx(0)
    assert by_member["OLD"].rate == pytest.approx(0)
    assert by_member["NEW"].before is None
    assert by_member["OLD"].after is None
    assert sum(d.delta for d in breakdown.drivers) == pytest.approx(result.delta)


def test_ratio_other_row_and_unassigned_still_add_up() -> None:
    members = {name: (i + 1, 10) for i, name in enumerate("ABCDEF")}
    after = {name: (i + 3, 10) for i, name in enumerate("ABCDEF")}
    runner = fake_runner({BEFORE.start: rates(**members), AFTER.start: rates(**after)})
    result = metric_drivers.explain(runner, SPECS, "late_rate", BEFORE, AFTER, [DIM], top=3)
    (breakdown,) = result.breakdowns
    assert len(breakdown.drivers) == 3
    assert breakdown.other is not None
    assert sum(d.delta for d in breakdown.drivers) + breakdown.other.delta == pytest.approx(result.delta)


def test_ratio_without_a_denominator_is_undefined() -> None:
    runner = fake_runner({BEFORE.start: rates(A=(0, 0)), AFTER.start: rates(A=(1, 10))})
    with pytest.raises(ValueError, match="undefined"):
        metric_drivers.explain(runner, SPECS, "late_rate", BEFORE, AFTER, [DIM])


# --- threshold -----------------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("before", "after", "base", "expected"),
    [
        (100, 110, 500, True),  # exactly 10%
        (100, 109, 500, False),
        (100, 85, 500, True),  # falls count too
        (100, 120, 99, False),  # earlier month too small
        (100, 120, 100, True),
        (0, 5, 500, False),  # no relative change from zero
    ],
)
def test_threshold(before: float, after: float, base: float, expected: bool) -> None:
    assert metric_drivers.moved_past_threshold(before, after, base) is expected


def test_threshold_is_configurable() -> None:
    assert metric_drivers.moved_past_threshold(100, 104, 500, threshold=0.03)
    assert not metric_drivers.moved_past_threshold(100, 120, 500, min_base=1000)


# --- the run check and the email -------------------------------------------------------------------


def month_tables(extra_before: float = 0.0) -> dict[datetime.date, list[dict[str, Any]]]:
    def rows(sp: float, rj: float, orders: float, late: float, delivered: float) -> list[dict[str, Any]]:
        return [
            {DIM: "SP", "revenue": sp, "orders": orders * 0.6, "late": late * 0.5, "delivered": delivered * 0.6},
            {DIM: "RJ", "revenue": rj, "orders": orders * 0.4, "late": late * 0.5, "delivered": delivered * 0.4},
        ]

    return {BEFORE.start: rows(1000 + extra_before, 500, 500, 10, 450), AFTER.start: rows(1500, 520, 520, 40, 480)}


def checked(result: dict[str, Any]) -> dict[str, dict[str, Any]]:
    return {c["metric"]: c for c in result["checked"]}


def test_run_check_explains_only_what_moved() -> None:
    result = metric_drivers.run_check(
        fake_runner(month_tables()), SPECS, datetime.date(2017, 11, 1), metrics=["revenue", "orders", "late_rate"], dimensions=[DIM]
    )
    assert (result["before_period"], result["after_period"]) == ("2017-10", "2017-11")
    assert result["base"] == 500
    flags = {name: c["moved"] for name, c in checked(result).items()}
    assert flags == {"revenue": True, "orders": False, "late_rate": True}
    assert [m["metric"] for m in result["moved"]] == ["revenue", "late_rate"]
    revenue = result["moved"][0]
    rows = revenue["breakdowns"][0]["drivers"]
    assert sum(r["delta"] for r in rows) == pytest.approx(revenue["delta"])


def test_run_check_with_a_small_earlier_month_flags_nothing() -> None:
    tables = month_tables()
    result = metric_drivers.run_check(fake_runner(tables), SPECS, datetime.date(2017, 11, 1), min_base=10_000, metrics=["revenue"], dimensions=[DIM])
    assert result["moved"] == []
    assert metric_drivers.drivers_html(result) == ""
    assert metric_drivers.subject_hint(result) == ""


def test_run_check_keeps_the_two_most_telling_dimensions() -> None:
    # `flag` has two members, so its top members say nothing; `state` has more members than shown
    tables = {
        BEFORE.start: [{DIM: s, "flag": f, "g": f, "revenue": 100.0} for s, f in zip("ABCDEF", "xyxyxy", strict=True)],
        AFTER.start: [{DIM: s, "flag": f, "g": f, "revenue": 100.0 + (300 if s == "A" else 0)} for s, f in zip("ABCDEF", "xyxyxy", strict=True)],
    }

    def run(metrics: list[str], group_by: list[str], start: datetime.date, end: datetime.date) -> list[dict[str, Any]]:
        rows = tables[start]
        if not group_by:
            return [{m: sum(r.get(m, 0) for r in rows) for m in metrics}]
        merged: dict[str, float] = {}
        for r in rows:
            merged[r[group_by[0]]] = merged.get(r[group_by[0]], 0) + r["revenue"]
        return [{group_by[0]: k, "revenue": v} for k, v in merged.items()]

    result = metric_drivers.run_check(run, SPECS, datetime.date(2017, 11, 1), min_base=0, metrics=["revenue"], dimensions=["flag", "g", DIM])
    shown = [b["dimension"] for b in result["moved"][0]["breakdowns"]]
    assert len(shown) == metric_drivers.EMAIL_DIMENSION_COUNT
    assert shown[0] == DIM


def test_email_block_shows_drivers_with_mix_and_rate_for_ratios() -> None:
    result = metric_drivers.run_check(
        fake_runner(month_tables()), SPECS, datetime.date(2017, 11, 1), metrics=["revenue", "late_rate"], dimensions=[DIM]
    )
    html = metric_drivers.drivers_html(result)
    assert "Metrics that moved: 2017-11 vs 2017-10" in html
    assert "revenue: 1,500 to 2,020" in html
    assert "by customer__state" in html
    assert ">SP<" in html
    assert ">mix<" in html  # only the ratio block has it
    assert html.count(">mix<") == 1
    assert "pp" in html  # a rate change in percentage points
    assert metric_drivers.subject_hint(result).startswith(", revenue +")


def test_email_block_escapes_member_names() -> None:
    drivers = {
        "before_period": "a",
        "after_period": "b",
        "threshold": 0.1,
        "base": 500,
        "moved": [
            {
                "metric": "revenue",
                "kind": "additive",
                "before": 1.0,
                "after": 2.0,
                "delta": 1.0,
                "relative": 1.0,
                "breakdowns": [
                    {
                        "dimension": "d",
                        "members": 1,
                        "explained": 1.0,
                        "other": None,
                        "drivers": [{"member": "<b>x</b>", "before": 1.0, "after": 2.0, "delta": 1.0, "mix": None, "rate": None, "share": 1.0}],
                    }
                ],
            }
        ],
    }
    html = metric_drivers.drivers_html(drivers)
    assert "<b>x</b>" not in html
    assert "&lt;b&gt;x&lt;/b&gt;" in html


# --- in the run email ----------------------------------------------------------------------------


def report(drivers: dict[str, Any] | None, tmp_path: Any) -> airflow_emails.Email:
    now = datetime.datetime(2026, 11, 9, 12, tzinfo=datetime.UTC)
    status = {
        "orders_loaded": 7544,
        "orders_total": 20000,
        "last_order_at": "2017-11-30T23:00:00",
        "last_loaded_at": now.isoformat(),
    }
    email, _ = airflow_emails.report_email(
        "duckdb", "run1", status, False, now, stale_after=24, log_dir=tmp_path, manifest=tmp_path / "none.json", drivers=drivers
    )
    assert email is not None
    return email


def test_ok_email_shows_the_drivers_when_a_metric_moved(tmp_path: Any) -> None:
    drivers = metric_drivers.run_check(
        fake_runner(month_tables()), SPECS, datetime.date(2017, 11, 1), metrics=["revenue"], dimensions=[DIM]
    )
    email = report(drivers, tmp_path)
    assert "Metrics that moved" in email.html
    assert "revenue +" in email.subject
    assert email.kind == "OK"


@pytest.mark.parametrize("drivers", [None, {"moved": []}])
def test_ok_email_without_drivers_is_unchanged(tmp_path: Any, drivers: dict[str, Any] | None) -> None:
    email = report(drivers, tmp_path)
    assert "Metrics that moved" not in email.html
    assert email.subject == "[olist_elt_duckdb] OK: +7544 orders, data up to 2017-11"


# --- time ------------------------------------------------------------------------------------------


def test_months_and_labels() -> None:
    assert metric_drivers.previous_month(datetime.date(2018, 1, 1)) == datetime.date(2017, 12, 1)
    assert metric_drivers.month_period(datetime.date(2016, 2, 1)).end == datetime.date(2016, 2, 29)
    assert metric_drivers.month_period(datetime.date(2017, 12, 1)).label == "2017-12"
    assert Period(datetime.date(2017, 10, 5), datetime.date(2017, 11, 5)).label == "2017-10-05 to 2017-11-05"


def test_load_engine_reads_the_given_manifest(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """#269: MetricFlow gets the manifest the task parsed for its warehouse, not `<project>/target/`.

    The fixture names catalog `airflow` (the Airflow DuckDB file); the project dir has no target/.
    No warehouse: explain only renders SQL.
    """
    from dbt.adapters.factory import FACTORY
    from dbt_metricflow.cli.dbt_connectors import dbt_config_accessor
    from metricflow.engine.metricflow_engine import MetricFlowExplainResult, MetricFlowQueryRequest

    class Adapter:
        @staticmethod
        def type() -> str:
            return "duckdb"

    class Credentials:
        type = "duckdb"

    class Metadata:
        profile = type("Profile", (), {"credentials": Credentials})

    def load_from_paths(profiles_path: Path, project_path: Path) -> Metadata:
        assert project_path == tmp_path
        return Metadata()

    monkeypatch.setattr(dbt_config_accessor.dbtProjectMetadata, "load_from_paths", staticmethod(load_from_paths))
    monkeypatch.setattr(FACTORY, "lookup_adapter", lambda adapter_type: Adapter())
    monkeypatch.delenv("DBT_TARGET", raising=False)
    manifest = Path(__file__).parent / "fixtures" / "semantic_manifest_airflow.json"

    engine = metric_drivers.load_engine("dev", tmp_path, manifest)

    explained: MetricFlowExplainResult = engine.explain(MetricFlowQueryRequest.create(metric_names=["orders"], group_by_names=["order__order_status"]))
    sql = explained.sql_statement.sql
    assert '"airflow"."marts"."fct_orders"' in sql or "airflow.marts.fct_orders" in sql, sql
    assert "olist" not in sql

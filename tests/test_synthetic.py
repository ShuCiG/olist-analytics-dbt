"""ingestion/synthetic.py and the loader's feed after the dump (D112).

The dump is fault_injection's clean one (2018-01 to 2018-06, every data test passes
on it), so the generated months come from a consistent Olist-format dump.
"""

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import mean
from typing import cast

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pytest

from fault_injection.dump import clean_rows, write_dump
from ingestion import pipeline, synthetic


@pytest.fixture(scope="module")
def dump_dir(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return write_dump(clean_rows(), tmp_path_factory.mktemp("synthetic") / "kaggle")


@pytest.fixture(scope="module")
def dump(dump_dir: Path) -> synthetic.Dump:
    source = pipeline.SourceDump(dump_dir)
    return synthetic.Dump({table: source.table(table).drop_columns(
        [pipeline.SYNTHETIC_FLAG] if table == "orders" else []
    ) for table in synthetic.TABLES})


def column(table: pa.Table, name: str) -> list[str]:
    return synthetic.strings(table[name])


def test_months_between() -> None:
    assert synthetic.months_between("2018-10", "2019-02") == ["2018-11", "2018-12", "2019-01", "2019-02"]
    assert synthetic.months_between("2018-10", "2018-10") == []


def test_shift_days_keeps_nulls() -> None:
    shifted = synthetic.shift_days(pa.chunked_array([["2017-12-31 23:59:59", None]]), 366)
    assert shifted.to_pylist() == ["2019-01-01 23:59:59", None]


def test_template_is_the_latest_full_month_of_the_same_calendar_month() -> None:
    orders = pa.table({
        "order_id": [f"o{i}" for i in range(31)],
        "customer_id": [f"c{i}" for i in range(31)],
        # 2017-11: 10 orders, 2018-11: 1 (under 10 % of the busiest), 2018-01: 20
        "order_purchase_timestamp": ["2017-11-05 10:00:00"] * 10 + ["2018-11-01 10:00:00"] + ["2018-01-05 10:00:00"] * 20,
    })
    empty = {name: pa.table({"order_id": pa.array([], pa.string())}) for name in synthetic.ORDER_CHILDREN}
    customers = pa.table({"customer_id": ["c0"], "customer_unique_id": ["p0"]})
    sellers = pa.table({"seller_id": pa.array([], pa.string())})
    dump = synthetic.Dump({"orders": orders, **empty, "customers": customers, "sellers": sellers})
    assert dump.template_month("2019-11") == "2017-11"
    # No full month 03 in the dump: its latest full month
    assert dump.template_month("2019-03") == "2018-01"


def test_a_month_comes_out_the_same_every_time(dump: synthetic.Dump) -> None:
    first, again = synthetic.generate_month(dump, "2018-07"), synthetic.generate_month(dump, "2018-07")
    assert all(first[name].equals(again[name]) for name in synthetic.TABLES)
    other = synthetic.generate_month(dump, "2018-08")
    assert set(column(first["orders"], "order_id")).isdisjoint(column(other["orders"], "order_id"))


def test_months_inside_the_dump_are_refused(dump: synthetic.Dump) -> None:
    with pytest.raises(ValueError, match="inside the dump"):
        synthetic.generate_month(dump, "2018-06")


def test_generated_rows_fit_together(dump: synthetic.Dump) -> None:
    month = synthetic.generate_month(dump, "2018-07")
    order_ids = column(month["orders"], "order_id")
    assert len(set(order_ids)) == len(order_ids)
    assert set(order_ids).isdisjoint(column(dump.tables["orders"], "order_id"))
    for name in synthetic.ORDER_CHILDREN:
        assert set(column(month[name], "order_id")) <= set(order_ids), name
    # Every order has its own customer row; repeat orders reuse a person of the dump
    assert column(month["customers"], "customer_id") == column(month["orders"], "customer_id")
    # Every item's seller is in the dump or among this month's new sellers
    known = set(column(dump.tables["sellers"], "seller_id")) | set(column(month["sellers"], "seller_id"))
    assert set(column(month["order_items"], "seller_id")) <= known
    assert month["sellers"].num_rows >= 1
    assert set(column(month["sellers"], "seller_id")) & set(column(month["order_items"], "seller_id"))
    # Reviews keep the template's id (its label joins on it), keys stay unique
    assert set(column(month["order_reviews"], "review_id")) <= set(column(dump.tables["order_reviews"], "review_id"))
    keys = list(zip(column(month["order_reviews"], "review_id"), column(month["order_reviews"], "order_id"), strict=True))
    assert len(set(keys)) == len(keys)
    assert all(t.schema == dump.tables[name].schema for name, t in month.items())


def test_generated_orders_are_in_their_month_with_the_dump_lags(dump: synthetic.Dump) -> None:
    month = synthetic.generate_month(dump, "2019-07")  # 13 months after its template
    purchased = column(month["orders"], "order_purchase_timestamp")
    assert {value[:7] for value in purchased} == {"2019-07"}

    def lags(orders: pa.Table) -> list[float]:
        start = pc.strptime(orders["order_purchase_timestamp"], "%Y-%m-%d %H:%M:%S", "s")
        end = pc.strptime(orders["order_delivered_customer_date"], "%Y-%m-%d %H:%M:%S", "s")
        durations = cast(list[timedelta | None], pc.subtract(end, start).to_pylist())
        return [d.total_seconds() / 86_400 for d in durations if d is not None]

    template = dump.tables["orders"].take(dump.orders_by_month[dump.template_month("2019-07")])
    assert abs(mean(lags(month["orders"])) - mean(lags(template))) < 0.5


# Tolerances of generated months against their template month (D112). The test dump
# has ~150 orders a month, so shares are compared over six generated months
def shares(values: list[str]) -> dict[str, float]:
    return {value: values.count(value) / len(values) for value in set(values)}


def test_generated_months_follow_their_template(dump: synthetic.Dump) -> None:
    template_rows = dump.orders_by_month["2018-06"]  # the template of every month here
    for month in synthetic.months_between("2018-06", "2018-12"):
        assert dump.template_month(month) == "2018-06"
        count = synthetic.generate_month(dump, month)["orders"].num_rows
        assert abs(count / len(template_rows) - 1) <= synthetic.VOLUME_JITTER + 0.01, month
    generated = synthetic.generate(dump, "2018-12")
    template_ids = pa.array(sorted(column(dump.tables["orders"].take(template_rows), "order_id")))

    def of_template(name: str) -> pa.Table:
        return dump.tables[name].filter(pc.is_in(dump.tables[name]["order_id"], template_ids))

    orders = generated["orders"].num_rows
    items_per_order = generated["order_items"].num_rows / orders
    assert abs(items_per_order - of_template("order_items").num_rows / len(template_rows)) < 0.05
    for name, field in [("orders", "order_status"), ("order_payments", "payment_type"), ("order_reviews", "review_score")]:
        made, real = shares(column(generated[name], field)), shares(column(of_template(name), field))
        assert all(abs(made.get(k, 0) - real.get(k, 0)) < 0.05 for k in real | made), (name, field, made, real)
    real_people = set(column(dump.tables["customers"], "customer_unique_id"))
    repeat = sum(person in real_people for person in column(generated["customers"], "customer_unique_id")) / orders
    assert abs(repeat - dump.repeat_share) < 0.02


NOW = datetime(2026, 11, 11, 12, 0, tzinfo=UTC)


@pytest.mark.parametrize(
    ("cursor", "until", "next_only", "every", "hours_ago", "expected"),
    [
        ("2018-05", None, True, 24, 30, None),  # dump not used up: its own next month
        ("2018-06", None, True, None, 30, None),  # no flag: nothing generated
        ("2018-06", None, True, 24, 30, "2018-07"),  # used up and due
        ("2018-06", None, True, 24, 23.6, "2018-07"),  # within the slack
        ("2018-06", None, True, 24, 23, None),  # not due yet
        ("2018-08", None, True, 24, 2, "2018-08"),  # not due: keeps the loaded months
        ("2018-08", None, False, None, 2, "2018-08"),  # without the flag, too
        ("2018-08", None, True, 24, 30, "2018-09"),
        ("2018-03", "2018-09", False, 24, 1, "2018-09"),  # --until past the dump
        ("2018-03", "2018-09", False, None, 1, None),  # ... only with the flag
        (None, None, True, 24, None, None),  # first load: the dump first
    ],
)
def test_synthetic_until(
    cursor: str | None, until: str | None, next_only: bool, every: float | None, hours_ago: float | None, expected: str | None
) -> None:
    last_loaded_at = NOW - timedelta(hours=hours_ago) if hours_ago is not None else None
    assert pipeline.synthetic_until("2018-06", cursor, until, next_only, every, last_loaded_at, NOW) == expected


def test_naive_load_time_counts_as_utc() -> None:
    naive = (NOW - timedelta(hours=30)).replace(tzinfo=None)
    assert pipeline.synthetic_until("2018-06", "2018-06", None, True, 24, naive, NOW) == "2018-07"


@pytest.fixture
def run(dump_dir: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    """pipeline.main() on the clean dump; returns the feed status."""
    db = tmp_path / "test.duckdb"
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.setitem(sys.modules, "anthropic", None)
    monkeypatch.setattr(pipeline.kagglehub, "dataset_download", lambda _: str(dump_dir))
    labels = tmp_path / "labels"
    labels.mkdir()
    monkeypatch.setattr(pipeline, "LABELS_DIR", labels)

    def _run(*args: str) -> dict:
        monkeypatch.setattr(sys, "argv", ["pipeline.py", "--duckdb-path", str(db), *args])
        pipeline.main()
        return json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    _run.db = db  # type: ignore[attr-defined]
    return _run


def query(db: Path, sql: str) -> list[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(sql).fetchall()


def test_next_past_the_dump_loads_generated_months(run) -> None:
    run()  # the whole dump
    assert query(run.db, "select distinct _is_synthetic from raw.orders") == [(False,)]
    # Without the flag the used-up dump loads nothing, as before
    assert run("--next")["orders_loaded"] == 0
    july = run("--next", "--synthetic-every-hours", "0")
    assert july["orders_loaded"] > 0 and july["last_order_at"][:7] == "2018-07"
    august = run("--next", "--synthetic-every-hours", "0")
    assert august["last_order_at"][:7] == "2018-08"
    by_month = dict(query(run.db, """
        select left(order_purchase_timestamp, 7), bool_and(_is_synthetic)
        from raw.orders group by 1 having left(order_purchase_timestamp, 7) >= '2018-06'
    """))
    assert by_month == {"2018-06": False, "2018-07": True, "2018-08": True}
    # The customers snapshot keeps July's customers after August's load (replace)
    orphans = query(run.db, """
        select count(*) from raw.orders
        where customer_id not in (select customer_id from raw.customers)
    """)
    assert orphans == [(0,)]
    sellers = query(run.db, """
        select count(*) from raw.order_items
        where seller_id not in (select seller_id from raw.sellers)
    """)
    assert sellers == [(0,)]


def test_generated_month_waits_for_the_interval(run) -> None:
    run()
    assert run("--next", "--synthetic-every-hours", "24")["orders_loaded"] == 0  # last load: just now
    assert query(run.db, "select count(*) from raw.orders where _is_synthetic") == [(0,)]


def test_until_past_the_dump_generates_every_month_up_to_it(run, monkeypatch: pytest.MonkeyPatch) -> None:
    status = run("--until", "2018-09", "--synthetic-every-hours", "24")
    assert status["last_order_at"][:7] == "2018-09"
    months = query(run.db, "select count(distinct left(order_purchase_timestamp, 7)) from raw.orders where _is_synthetic")
    assert months == [(3,)]
    # A later load without the flag (here: one retrying a failed run's packages)
    # replaces the customers snapshot and keeps the generated customers in it
    monkeypatch.setattr(pipeline.dlt.Pipeline, "has_pending_data", property(lambda _: True))
    run("--next")
    generated_customers = "select count(*) from raw.customers where customer_id in (select customer_id from raw.orders where _is_synthetic)"
    assert query(run.db, generated_customers) == query(run.db, "select count(*) from raw.orders where _is_synthetic")


def test_months_of_another_generator_version_stop_the_load(run, monkeypatch: pytest.MonkeyPatch) -> None:
    run("--until", "2018-07", "--synthetic-every-hours", "0")
    # The loader's own import of the module (pipeline.py puts ingestion/ on sys.path)
    monkeypatch.setattr(pipeline.synthetic, "VERSION", pipeline.synthetic.VERSION + 1)
    with pytest.raises(SystemExit, match="--full-refresh"):
        run("--next", "--synthetic-every-hours", "0")
    # A full refresh starts over with the dump; nothing generated is kept
    run("--full-refresh")
    assert query(run.db, "select count(*) from raw.orders where _is_synthetic") == [(0,)]
    assert run("--next", "--synthetic-every-hours", "0")["last_order_at"][:7] == "2018-07"

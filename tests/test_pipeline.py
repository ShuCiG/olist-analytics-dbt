"""ingestion/pipeline.py end to end on a tiny fake dump: chunk cursor, `--next`,
`--until`, `--full-refresh` and the feed status JSON that Airflow reads.

Each test gets its own DuckDB file and dlt state dir (DLT_DATA_DIR); Kaggle is
replaced by CSVs written here.
"""

import csv
import json
import sys
from pathlib import Path

import duckdb
import pytest

from ingestion import pipeline

# Purchase months: 2016-09 (2 orders), 2016-10 (1), none in 2016-11, 2016-12 (1);
# o5 has no purchase time and never loads
ORDERS = [
    ("o1", "2016-09-04 21:15:19"),
    ("o2", "2016-09-15 12:16:38"),
    ("o3", "2016-10-02 22:07:52"),
    ("o4", "2016-12-23 23:16:47"),
    ("o5", ""),
]
ITEMS_PER_ORDER = {"o1": 2, "o2": 1, "o3": 1, "o4": 3, "o5": 1}


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


@pytest.fixture
def dump(tmp_path: Path) -> Path:
    src = tmp_path / "kaggle"
    src.mkdir()
    for file_name in pipeline.REFERENCE_TABLES.values():
        write_csv(src / file_name, ["id", "zip_code_prefix"], [("r1", "01001"), ("r2", "99990")])
    write_csv(
        src / "olist_orders_dataset.csv",
        ["order_id", "order_purchase_timestamp", "order_status"],
        [(order_id, ts, "delivered") for order_id, ts in ORDERS],
    )
    write_csv(
        src / "olist_order_items_dataset.csv",
        ["order_id", "order_item_id", "price"],
        [(o, str(i), "10.00") for o, n in ITEMS_PER_ORDER.items() for i in range(1, n + 1)],
    )
    write_csv(
        src / "olist_order_payments_dataset.csv",
        ["order_id", "payment_sequential", "payment_value"],
        [(o, "1", "10.00") for o, _ in ORDERS],
    )
    write_csv(
        src / "olist_order_reviews_dataset.csv",
        ["review_id", "order_id", "review_score"],
        [(f"rv_{o}", o, "5") for o, _ in ORDERS],
    )
    return src


@pytest.fixture
def run(dump: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]):
    """Run pipeline.main() with CLI args; returns the feed status (last stdout line)."""
    db = tmp_path / "test.duckdb"
    monkeypatch.setenv("DLT_DATA_DIR", str(tmp_path / "dlt"))
    monkeypatch.setattr(pipeline.kagglehub, "dataset_download", lambda _: str(dump))

    def _run(*args: str) -> dict:
        monkeypatch.setattr(sys, "argv", ["pipeline.py", "--duckdb-path", str(db), *args])
        pipeline.main()
        return json.loads(capsys.readouterr().out.strip().splitlines()[-1])

    _run.db = db  # type: ignore[attr-defined]
    return _run


def raw_count(db: Path, table: str) -> int:
    with duckdb.connect(str(db), read_only=True) as con:
        row = con.execute(f"select count(*) from raw.{table}").fetchone()
    assert row is not None
    return row[0]


def test_next_month() -> None:
    assert pipeline.next_month("2017-01") == "2017-02"
    assert pipeline.next_month("2017-12") == "2018-01"


@pytest.mark.parametrize("value", ["2017-1", "2017-13", "2017-00", "17-01", "2017-01-01"])
def test_month_key_rejects(value: str) -> None:
    with pytest.raises(Exception, match="expected YYYY-MM"):
        pipeline.month_key(value)


def test_until_loads_up_to_month(run) -> None:
    status = run("--until", "2016-09")
    assert status["orders_loaded"] == 2
    assert status["orders_total"] == 2
    assert status["last_order_at"] == "2016-09-15 12:16:38"
    assert status["last_loaded_at"] is not None
    # Child tables follow their orders; reference tables load in full
    assert raw_count(run.db, "order_items") == 3
    assert raw_count(run.db, "order_payments") == 2
    assert raw_count(run.db, "customers") == 2


def test_next_skips_empty_months_until_dump_is_used_up(run) -> None:
    run("--until", "2016-09")
    assert run("--next")["last_order_at"] == "2016-10-02 22:07:52"
    # 2016-11 has no orders: the cursor jumps to the next month that has some
    status = run("--next")
    assert (status["orders_loaded"], status["orders_total"]) == (1, 4)
    assert status["last_order_at"] == "2016-12-23 23:16:47"
    # Used up: nothing new, and `last_loaded_at` stays (freshness = new orders only)
    exhausted = run("--next")
    assert (exhausted["orders_loaded"], exhausted["orders_total"]) == (0, 4)
    assert exhausted["last_loaded_at"] == status["last_loaded_at"]
    assert raw_count(run.db, "order_items") == 7  # o5 (no purchase time) never loads


def test_next_from_empty_loads_first_month(run) -> None:
    status = run("--next")
    assert (status["orders_loaded"], status["last_order_at"][:7]) == (2, "2016-09")


def test_rerun_of_loaded_month_is_a_no_op(run) -> None:
    run("--until", "2016-10")
    status = run("--until", "2016-10")
    assert (status["orders_loaded"], status["orders_total"]) == (0, 3)
    assert raw_count(run.db, "order_items") == 4  # merge on the key: no duplicates


def test_full_refresh_drops_data_and_cursor(run) -> None:
    run()  # everything
    status = run("--full-refresh", "--until", "2016-09")
    assert (status["orders_loaded"], status["orders_total"]) == (2, 2)
    assert run("--next")["last_order_at"][:7] == "2016-10"


def test_feed_status_rejects_a_non_timestamp(run, dump: Path) -> None:
    # Sorts last, so it becomes max(order_purchase_timestamp): Airflow would render it
    # into a bash command (Elementary's clock); the ingest must fail instead
    tampered = [*ORDERS, ("o6", "2016-12-31 23:59:59'; touch /tmp/pwned #")]
    write_csv(
        dump / "olist_orders_dataset.csv",
        ["order_id", "order_purchase_timestamp", "order_status"],
        [(order_id, ts, "delivered") for order_id, ts in tampered],
    )
    with pytest.raises(ValueError, match="not a timestamp"):
        run()

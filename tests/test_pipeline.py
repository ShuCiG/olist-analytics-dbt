"""ingestion/pipeline.py end to end on a tiny fake dump: chunk cursor, `--next`,
`--until`, `--full-refresh` and the feed status JSON that Airflow reads.

Each test gets its own DuckDB file and dlt state dir (DLT_DATA_DIR); Kaggle is
replaced by CSVs written here.
"""

import csv
import json
import re
import shutil
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


LABEL_HEADER = ["review_id", "sentiment", "topic", "translation_en", "labeled_by", "guidelines_version"]


def write_labels(path: Path, rows: list[tuple[str, str]]) -> None:
    write_csv(path, LABEL_HEADER, [(rid, s, "delivery", "Arrived, \"fast\",\nthanks", "test", "1") for rid, s in rows])


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
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)  # never the real API from a test
    monkeypatch.setitem(sys.modules, "anthropic", None)
    monkeypatch.setattr(pipeline.kagglehub, "dataset_download", lambda _: str(dump))
    labels = tmp_path / "labels"
    labels.mkdir()
    write_labels(labels / "batch_000.csv", [("rv_o1", "positive"), ("rv_o3", "negative")])
    monkeypatch.setattr(pipeline, "LABELS_DIR", labels)

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


def loaded_at(db: Path, table: str) -> object:
    with duckdb.connect(str(db), read_only=True) as con:
        row = con.execute(f"select max(_loaded_at) from raw.{table}").fetchone()
    assert row is not None
    return row[0]


def test_next_on_used_up_dump_skips_the_load(run) -> None:
    run()  # everything
    reloaded = loaded_at(run.db, "customers")
    status = run("--next")
    assert (status["orders_loaded"], status["orders_total"]) == (0, 4)
    # No dlt run at all: the reference tables were not replaced
    assert loaded_at(run.db, "customers") == reloaded


def test_full_refresh_next_on_used_up_dump_starts_over(run) -> None:
    run()  # everything
    status = run("--full-refresh", "--next")
    assert (status["orders_loaded"], status["orders_total"]) == (2, 2)


@pytest.mark.parametrize("args", [(), ("--next",), ("--until", "2016-12")])
def test_used_up_dump_skips_the_load_without_local_state(run, tmp_path: Path, args) -> None:
    run()  # everything
    reloaded = loaded_at(run.db, "customers")
    # Fresh runner or volume (CD deploy): the cursor comes from the destination
    shutil.rmtree(tmp_path / "dlt")
    status = run(*args)
    assert (status["orders_loaded"], status["orders_total"]) == (0, 4)
    assert loaded_at(run.db, "customers") == reloaded


def test_until_already_loaded_skips_the_load(run) -> None:
    run("--until", "2016-10")
    reloaded = loaded_at(run.db, "customers")
    # 2016-12 is in the dump but after `--until`: nothing to load either
    assert run("--until", "2016-11")["orders_loaded"] == 0
    assert loaded_at(run.db, "customers") == reloaded
    # A later `--until` loads again, from the restored cursor
    status = run("--until", "2016-12")
    assert (status["orders_loaded"], status["orders_total"]) == (1, 4)


def test_new_month_in_the_dump_loads_without_local_state(run, tmp_path: Path) -> None:
    run("--until", "2016-10")
    shutil.rmtree(tmp_path / "dlt")
    status = run()
    assert (status["orders_loaded"], status["orders_total"]) == (1, 4)


@pytest.mark.parametrize("args", [(), ("--next",)])
def test_new_duckdb_file_loads_from_the_start_with_local_state(run, tmp_path: Path, args) -> None:
    run()  # everything; the local cursor now says 2016-12
    # The pipeline name is per destination and dataset, not per file (#152)
    status = run("--duckdb-path", str(tmp_path / "other.duckdb"), *args)
    assert status["orders_loaded"] == (4 if not args else 2)
    assert status["last_order_at"][:7] == ("2016-12" if not args else "2016-09")


def test_deleted_duckdb_file_loads_from_the_start(run) -> None:
    run()
    run.db.unlink()
    status = run("--next")
    assert (status["orders_loaded"], status["orders_total"]) == (2, 2)


def test_older_copy_of_duckdb_file_loads_from_its_own_cursor(run) -> None:
    run("--until", "2016-09")
    backup = run.db.with_name("backup.duckdb")
    shutil.copy(run.db, backup)
    run()  # everything
    shutil.copy(backup, run.db)
    # dlt keeps a local state newer than the destination's: it must not win here
    status = run()
    assert (status["orders_loaded"], status["orders_total"]) == (2, 4)


def test_next_loads_when_a_failed_run_left_packages(run, monkeypatch: pytest.MonkeyPatch) -> None:
    run()  # everything
    reloaded = loaded_at(run.db, "customers")
    # Only a load retries pending packages: the skip must not strand them
    monkeypatch.setattr(pipeline.dlt.Pipeline, "has_pending_data", property(lambda _: True))
    run("--next")
    assert loaded_at(run.db, "customers") != reloaded


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


def write_orders(dump: Path, orders: list[tuple[str, str]]) -> None:
    write_csv(
        dump / "olist_orders_dataset.csv",
        ["order_id", "order_purchase_timestamp", "order_status"],
        [(order_id, ts, "delivered") for order_id, ts in orders],
    )


@pytest.mark.parametrize(
    "bad",
    [
        # Sorts last: would become the cursor and Elementary's clock, which Airflow
        # renders into a bash command
        "2016-12-31 23:59:59'; touch /tmp/pwned #",
        # Sorts first: `--next` would pick month "2016-5-" and crash every hour
        "2016-5-01 10:00:00",
    ],
)
def test_malformed_purchase_time_stops_the_load(run, dump: Path, bad: str) -> None:
    run("--until", "2016-09")
    write_orders(dump, [*ORDERS, ("o6", bad)])
    with pytest.raises(Exception, match="malformed order_purchase_timestamp"):  # dlt wraps it
        run("--next")
    # Nothing loaded, cursor untouched: once the file is fixed the feed goes on
    write_orders(dump, ORDERS)
    assert run("--next")["last_order_at"][:7] == "2016-10"


def test_malformed_purchase_time_stops_a_used_up_next(run, dump: Path) -> None:
    run()  # everything: `--next` would skip the load from here on
    write_orders(dump, [*ORDERS, ("o6", "2016-5-01 10:00:00")])
    with pytest.raises(ValueError, match="malformed order_purchase_timestamp"):
        run("--next")


def test_conflicting_rows_of_one_key_stop_the_load(run, dump: Path) -> None:
    run("--until", "2016-09")
    orders = [(o, ts, "delivered") for o, ts in ORDERS]
    header = ["order_id", "order_purchase_timestamp", "order_status"]
    write_csv(dump / "olist_orders_dataset.csv", header, [*orders, ("o3", ORDERS[2][1], "canceled")])
    with pytest.raises(Exception, match="conflicting rows"):  # dlt wraps it
        run("--next")
    # Nothing loaded, cursor untouched: once the file is fixed the feed goes on
    write_csv(dump / "olist_orders_dataset.csv", header, orders)
    assert run("--next")["last_order_at"][:7] == "2016-10"


def test_identical_rows_of_one_key_load_once(run, dump: Path) -> None:
    orders = [(o, ts, "delivered") for o, ts in ORDERS]
    header = ["order_id", "order_purchase_timestamp", "order_status"]
    write_csv(dump / "olist_orders_dataset.csv", header, [*orders, orders[0]])
    run("--until", "2016-09")
    assert raw_count(run.db, "orders") == 2


def test_empty_reference_file_stops_the_load(run, dump: Path) -> None:
    write_csv(dump / pipeline.REFERENCE_TABLES["sellers"], ["id", "zip_code_prefix"], [])
    with pytest.raises(ValueError, match="no rows"):
        run()


def raw_columns(db: Path, table: str) -> list[str]:
    with duckdb.connect(str(db), read_only=True) as con:
        rows = con.execute(
            "select column_name from information_schema.columns where table_schema = 'raw' and table_name = ?",
            [table],
        ).fetchall()
    return [r[0] for r in rows]


def test_new_source_column_lands_in_raw_and_is_reported(run, dump: Path) -> None:
    # First load creates the tables: not a schema change
    assert run("--until", "2016-09")["new_columns"] == {}
    # The source gains a column (empty for the next chunk's orders, as when a
    # field is introduced) and a reference table gains one too
    write_csv(
        dump / "olist_orders_dataset.csv",
        ["order_id", "order_purchase_timestamp", "order_status", "order_channel"],
        [(order_id, ts, "delivered", "") for order_id, ts in ORDERS],
    )
    write_csv(dump / pipeline.REFERENCE_TABLES["sellers"], ["id", "zip_code_prefix", "seller_tier"], [("r1", "01001", "gold")])
    status = run("--next")
    assert status["new_columns"] == {"orders": ["order_channel"], "sellers": ["seller_tier"]}
    assert "order_channel" in raw_columns(run.db, "orders")
    # Reported once: the next load knows the column
    assert run("--next")["new_columns"] == {}


def test_full_refresh_is_not_a_schema_change(run) -> None:
    run("--until", "2016-09")
    assert run("--full-refresh")["new_columns"] == {}


def test_skipped_load_reports_no_new_columns(run) -> None:
    run()  # everything: `--next` skips the load from here on
    assert run("--next")["new_columns"] == {}


def test_removed_source_column_is_reported_while_missing(run, dump: Path) -> None:
    # Matching headers: nothing missing
    assert run("--until", "2016-09")["removed_columns"] == {}
    # The source drops a column of an order table and of a reference table
    write_csv(
        dump / "olist_orders_dataset.csv",
        ["order_id", "order_purchase_timestamp"],
        [(order_id, ts) for order_id, ts in ORDERS],
    )
    write_csv(dump / pipeline.REFERENCE_TABLES["sellers"], ["id"], [("r1",), ("r2",)])
    status = run("--next")
    assert status["removed_columns"] == {"orders": ["order_status"], "sellers": ["zip_code_prefix"]}
    assert status["new_columns"] == {}
    # dlt keeps the column in raw and loads nulls into the new rows
    with duckdb.connect(str(run.db), read_only=True) as con:
        assert con.execute("select order_id, order_status from raw.orders order by 1").fetchall() == [
            ("o1", "delivered"),
            ("o2", "delivered"),
            ("o3", None),
        ]
        assert con.execute("select count(*) from raw.sellers where zip_code_prefix is null").fetchone() == (2,)
    # Reported again on the next load: the file still lacks it
    assert run("--next")["removed_columns"] == {"orders": ["order_status"], "sellers": ["zip_code_prefix"]}


def test_skipped_load_reports_no_removed_columns(run, dump: Path) -> None:
    run()
    write_csv(dump / pipeline.REFERENCE_TABLES["sellers"], ["id"], [("r1",)])
    # No load, no nulls written: nothing to report
    assert run("--next")["removed_columns"] == {}


STAGING = Path(__file__).resolve().parents[1] / "transform" / "models" / "staging"


def test_staging_selects_source_columns_by_name() -> None:
    # A new raw column must stop at raw (D61): the `source` CTE is only read through
    # an explicit column list, never `select *` / `source.*`
    models = sorted(STAGING.rglob("stg_*.sql"))
    assert models
    for model in models:
        sql = model.read_text(encoding="utf-8").lower()
        assert re.search(r"\{\{\s*source\(", sql), f"{model.name}: expected a source() CTE"
        assert not re.search(r"select\s+\*\s+from\s+source\b|\bsource\.\*", sql), f"{model.name} passes source columns through"


def raw_labels(db: Path) -> list[tuple]:
    with duckdb.connect(str(db), read_only=True) as con:
        return con.execute(
            "select review_id, sentiment, translation_en, labels_sha from raw.review_labels order by review_id"
        ).fetchall()


def test_review_labels_load_with_the_first_run(run) -> None:
    run("--until", "2016-09")
    rows = raw_labels(run.db)
    assert [r[:2] for r in rows] == [("rv_o1", "positive"), ("rv_o3", "negative")]
    assert rows[0][2] == 'Arrived, "fast",\nthanks'  # quotes and newlines survive
    assert len({r[3] for r in rows}) == 1


def test_unchanged_review_labels_are_not_reloaded(run) -> None:
    assert run("--until", "2016-09")["labels_loaded"] == 2
    before = raw_labels(run.db)
    assert run("--until", "2016-09")["labels_loaded"] == 0  # also an order run that skips its load
    assert raw_labels(run.db) == before


def test_new_label_batch_reloads_on_a_skipped_order_run(run, tmp_path: Path) -> None:
    run()
    write_labels(tmp_path / "labels" / "batch_001.csv", [("rv_o4", "mixed")])
    status = run("--next")  # dump used up: orders skip, labels still load
    assert (status["orders_loaded"], status["labels_loaded"]) == (0, 3)
    rows = raw_labels(run.db)
    assert [r[0] for r in rows] == ["rv_o1", "rv_o3", "rv_o4"]
    assert len({r[3] for r in rows}) == 1


def test_null_like_label_text_stays_text(tmp_path: Path) -> None:
    write_csv(tmp_path / "batch_001.csv", LABEL_HEADER, [("rv_1", "neutral", "general", "N/A", "test", "1")])
    table, _ = pipeline.read_review_labels(tmp_path)
    assert table.column("translation_en").to_pylist() == ["N/A"]


def test_new_review_with_a_labelled_text_gets_its_label(run, dump: Path) -> None:
    write_csv(
        dump / "olist_order_reviews_dataset.csv",
        ["review_id", "order_id", "review_score", "review_comment_title", "review_comment_message"],
        [("rv_o1", "o1", "5", "", "bom"), ("rv_o2", "o2", "5", "", "bom"), ("rv_o3", "o3", "1", "", "")]
        + [(f"rv_{o}", o, "1", "Atraso", "não chegou") for o in ("o4", "o5")],
    )
    run("--until", "2016-09")  # rv_o1 and rv_o3 are labelled (fixture); rv_o2 repeats rv_o1's text
    assert [(r[0], r[1]) for r in raw_labels(run.db)] == [
        ("rv_o1", "positive"), ("rv_o2", "positive"), ("rv_o3", "negative")
    ]
    status = run()  # rv_o4, rv_o5: a new text, no API key in tests -> left open, the run goes on
    assert status["labels_loaded"] == 0
    assert len(raw_labels(run.db)) == 3


def test_full_refresh_keeps_review_labels(run) -> None:
    run()
    run("--full-refresh")
    assert len(raw_labels(run.db)) == 2


def request_deletion(db: Path, *people: str, applied: bool = True) -> None:
    """Deletion requests as dbt's request_deletion makes them (D105), and by default
    the log row apply_deletion_requests writes: the loader leaves out applied requests."""
    with duckdb.connect(str(db)) as con:
        con.execute("create schema if not exists pii_keys")
        con.execute("create table if not exists pii_keys.deletion_requests (customer_unique_id varchar, requested_at timestamp)")
        for person in people:
            con.execute("insert into pii_keys.deletion_requests values (?, now()::timestamp)", [person])
        if applied:
            con.execute("create schema if not exists raw")
            con.execute(
                "create table if not exists raw.deletion_log"
                " (applied_at timestamp, requests_until timestamp, people integer, rows_deleted integer)"
            )
            con.execute(
                "insert into raw.deletion_log select now()::timestamp, max(requested_at), 0, 0 from pii_keys.deletion_requests"
            )


def with_people(dump: Path) -> None:
    """Customers per order (c_<order>), o1 and o4 by person p1, the rest one person each."""
    person = {"o1": "p1", "o2": "p2", "o3": "p3", "o4": "p1", "o5": "p5"}
    write_csv(
        dump / pipeline.REFERENCE_TABLES["customers"],
        ["customer_id", "customer_unique_id", "customer_zip_code_prefix"],
        [(f"c_{o}", p, "01001") for o, p in person.items()],
    )
    write_csv(
        dump / "olist_orders_dataset.csv",
        ["order_id", "customer_id", "order_purchase_timestamp", "order_status"],
        [(o, f"c_{o}", ts, "delivered") for o, ts in ORDERS],
    )


def raw_ids(db: Path, table: str, column: str) -> list[str]:
    with duckdb.connect(str(db), read_only=True) as con:
        return sorted(row[0] for row in con.execute(f"select {column} from raw.{table}").fetchall())


def test_deletion_request_leaves_the_person_out_of_every_load(run, dump: Path) -> None:
    with_people(dump)
    run("--until", "2016-09")
    request_deletion(run.db, "p1")
    # o4 (2016-12) of p1 arrives after the request, the customers are replaced
    status = run()
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p2", "p3", "p5"]
    # o1 was loaded before the request: apply_deletion_requests removes it, not the loader
    assert raw_ids(run.db, "orders", "order_id") == ["o1", "o2", "o3"]
    assert raw_ids(run.db, "order_items", "order_id") == ["o1", "o1", "o2", "o3"]
    # The cursor still reaches the month of the left-out order
    assert status["orders_total"] == 3
    assert pipeline.loaded_until(pipeline.dlt.pipeline("olist_duckdb")) == "2016-12"


def test_full_refresh_with_a_deletion_request_loads_everyone_else(run, dump: Path) -> None:
    with_people(dump)
    run("--until", "2016-09")
    request_deletion(run.db, "p1", "p1", "nobody")
    run("--full-refresh")
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p2", "p3", "p5"]
    assert raw_ids(run.db, "orders", "order_id") == ["o2", "o3"]
    for table in ("order_items", "order_payments", "order_reviews"):
        assert "o1" not in raw_ids(run.db, table, "order_id") and "o4" not in raw_ids(run.db, table, "order_id")


def test_pending_deletion_request_still_loads_the_person(run, dump: Path) -> None:
    with_people(dump)
    run("--until", "2016-09")
    # Applied: p2. Then p1 asks, and no apply ran yet: leaving p1's customer rows out
    # now would hide the orders of p1 from the apply to come
    request_deletion(run.db, "p2")
    request_deletion(run.db, "p1", applied=False)
    run()
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p1", "p1", "p3", "p5"]
    assert "o4" in raw_ids(run.db, "orders", "order_id")


def test_deletion_request_with_an_unfinished_apply_is_pending(run, dump: Path) -> None:
    with_people(dump)
    run("--until", "2016-09")
    request_deletion(run.db, "p1", applied=False)
    with duckdb.connect(str(run.db)) as con:
        # The row apply opens and closes only at its end
        con.execute("create table raw.deletion_log (applied_at timestamp, requests_until timestamp, people integer, rows_deleted integer)")
        con.execute("insert into raw.deletion_log select null, max(requested_at), 1, null from pii_keys.deletion_requests")
    run()
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p1", "p1", "p2", "p3", "p5"]


def test_deletion_log_table_name() -> None:
    assert pipeline.deletion_log_table("duckdb", "raw") == "raw.deletion_log"
    assert pipeline.deletion_log_table("bigquery", "airflow_raw") == f"`{pipeline.GCP_PROJECT_ID}.airflow_raw.deletion_log`"
    with pytest.raises(ValueError, match="dataset"):
        pipeline.deletion_log_table("duckdb", "raw; drop")


def test_without_a_request_table_everyone_loads(run, dump: Path) -> None:
    with_people(dump)
    run()
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p1", "p1", "p2", "p3", "p5"]


def test_required_request_table_missing_stops_the_load(run, dump: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with_people(dump)
    run("--until", "2016-09")
    request_deletion(run.db, "p1")
    monkeypatch.setenv("OLIST_DELETION_REQUESTS_REQUIRED", "bigquery, duckdb")
    run("--until", "2016-10")  # the table is there: as without the requirement
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p2", "p3", "p5"]
    reloaded = loaded_at(run.db, "customers")
    # A mistyped name (or a dropped table): p1 would come back with the customers
    monkeypatch.setenv("OLIST_DELETION_REQUESTS", "pii_keys.deletion_request")
    with pytest.raises(RuntimeError, match="pii_keys.deletion_request not found"):
        run()
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p2", "p3", "p5"]
    assert loaded_at(run.db, "customers") == reloaded
    assert raw_ids(run.db, "orders", "order_id") == ["o1", "o2", "o3"]


def test_request_table_missing_after_an_applied_request_stops_the_load(
    run, dump: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with_people(dump)
    run("--until", "2016-09")
    request_deletion(run.db, "p1")
    monkeypatch.delenv("OLIST_DELETION_REQUESTS_REQUIRED", raising=False)
    # No variable needed: the log says people were deleted here, so the table must exist
    monkeypatch.setenv("OLIST_DELETION_REQUESTS", "pii_keys.deletion_request")
    with pytest.raises(RuntimeError, match="raw.deletion_log holds applied requests"):
        run()
    assert raw_ids(run.db, "orders", "order_id") == ["o1", "o2"]


def test_request_table_missing_with_only_an_unfinished_apply_loads_everyone(
    run, dump: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with_people(dump)
    run("--until", "2016-09")
    with duckdb.connect(str(run.db)) as con:
        con.execute("create table raw.deletion_log (applied_at timestamp, requests_until timestamp, people integer, rows_deleted integer)")
        con.execute("insert into raw.deletion_log values (null, now()::timestamp, 1, null)")
    monkeypatch.delenv("OLIST_DELETION_REQUESTS_REQUIRED", raising=False)
    run()
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p1", "p1", "p2", "p3", "p5"]


def test_required_request_table_is_per_destination(run, dump: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    with_people(dump)
    monkeypatch.setenv("OLIST_DELETION_REQUESTS_REQUIRED", "bigquery")
    run()  # DuckDB is not named: no table, everyone loads
    assert raw_ids(run.db, "customers", "customer_unique_id") == ["p1", "p1", "p2", "p3", "p5"]


@pytest.mark.parametrize(
    ("value", "destination", "required"),
    [("", "bigquery", False), ("bigquery", "bigquery", True), ("bigquery", "duckdb", False), (" duckdb ,bigquery,", "duckdb", True)],
)
def test_deletion_requests_required(monkeypatch: pytest.MonkeyPatch, value: str, destination: str, required: bool) -> None:
    monkeypatch.setenv("OLIST_DELETION_REQUESTS_REQUIRED", value)
    assert pipeline.deletion_requests_required(destination) is required


@pytest.mark.parametrize("value", ["true", "1", "bigquery,prod", "big query"])
def test_deletion_requests_required_refuses_an_unknown_destination(monkeypatch: pytest.MonkeyPatch, value: str) -> None:
    monkeypatch.setenv("OLIST_DELETION_REQUESTS_REQUIRED", value)
    with pytest.raises(ValueError, match="must list destinations"):
        pipeline.deletion_requests_required("bigquery")


@pytest.mark.parametrize("name", ["pii_keys", "a.b.c", "x.y; drop table raw.orders", "a.b`"])
def test_deletion_request_table_name_is_checked(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    monkeypatch.setenv("OLIST_DELETION_REQUESTS", name)
    with pytest.raises(ValueError, match="schema.table"):
        pipeline.deletion_requests_table("duckdb")


def test_deletion_request_table_on_bigquery_is_in_the_project(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("OLIST_DELETION_REQUESTS", raising=False)
    assert pipeline.deletion_requests_table("bigquery") == f"`{pipeline.GCP_PROJECT_ID}.pii_keys.deletion_requests`"
    assert pipeline.deletion_requests_table("duckdb") == "pii_keys.deletion_requests"

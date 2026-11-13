"""A deletion request (D105) end to end on DuckDB, from the fault-injection baseline.

Two people of the synthetic dump ask to be deleted: one whose order is already
loaded (May: raw, the incremental fct_order_items, the rebuilt tables) and one
whose order is in June, the month the next load brings (the loader must leave
them out). After the deletion, the rebuild and June's load, the dbt check passes
and a scan of every text column of every table finds none of their values.
"""

import hashlib
from pathlib import Path

import duckdb
import pytest

from fault_injection.dump import LAST_CLEAN_MONTH, clean_rows, write_dump
from fault_injection.runner import AS_OF_NEW, copy_baseline, db_path, dbt, ingest

# The request table itself holds the ids, on purpose: it keeps them out of later loads
REQUESTS_SCHEMA = "pii_keys"


def person_values() -> tuple[list[str], set[str]]:
    """Two requested people (one per path) and every value that identifies them."""
    rows = clean_rows()
    month = {o["order_id"]: o["order_purchase_timestamp"][:7] for o in rows["orders"]}
    customer_of = {o["order_id"]: o["customer_id"] for o in rows["orders"]}
    person_of = {c["customer_id"]: c["customer_unique_id"] for c in rows["customers"]}
    loaded = next(o for o in reversed(rows["orders"]) if month[o["order_id"]] == LAST_CLEAN_MONTH)
    arriving = next(o for o in rows["orders"] if month[o["order_id"]] > LAST_CLEAN_MONTH)
    people = [person_of[customer_of[o["order_id"]]] for o in (loaded, arriving)]

    customer_ids = {c for c, p in person_of.items() if p in people}
    order_ids = {o for o, c in customer_of.items() if c in customer_ids}
    review_ids = {r["review_id"] for r in rows["order_reviews"] if r["order_id"] in order_ids}
    # customer_key on DuckDB: the fixed development key (macros/customer_key.sql)
    keys = {hashlib.sha256(f"olist-dev-key|{p}".encode()).hexdigest() for p in people}
    return people, set(people) | customer_ids | order_ids | review_ids | keys


def tables_holding(db: Path, values: set[str]) -> dict[str, int]:
    """Every text column of every table outside the request schema: matching rows."""
    found: dict[str, int] = {}
    with duckdb.connect(str(db), read_only=True) as con:
        columns = con.execute(
            """
            select c.table_schema, c.table_name, c.column_name
            from information_schema.columns c
            join information_schema.tables t using (table_catalog, table_schema, table_name)
            where t.table_type = 'BASE TABLE' and c.data_type = 'VARCHAR' and c.table_schema <> ?
            """,
            [REQUESTS_SCHEMA],
        ).fetchall()
        for schema, table, column in columns:
            row = con.execute(
                f'select count(*) from "{schema}"."{table}" where "{column}" in (select unnest(?))',
                [sorted(values)],
            ).fetchone()
            if row and row[0]:
                found[f"{schema}.{table}.{column}"] = row[0]
    return found


def raw_orders(db: Path) -> int:
    with duckdb.connect(str(db), read_only=True) as con:
        row = con.execute("select count(*) from raw.orders").fetchone()
    assert row is not None
    return row[0]


@pytest.fixture(scope="module")
def deleted(baseline: Path, tmp_path_factory: pytest.TempPathFactory) -> tuple[Path, set[str], list[str]]:
    """The baseline after the requests, apply, June's load and the rebuild; check outputs."""
    work = tmp_path_factory.mktemp("del")
    copy_baseline(baseline, work)
    people, values = person_values()
    outputs = []
    for person in people:
        dbt(work, AS_OF_NEW, "run-operation", "request_deletion", "--args", f"{{customer_unique_id: {person}}}")
    outputs.append(dbt(work, AS_OF_NEW, "run-operation", "check_deletion_requests"))
    applied = dbt(work, AS_OF_NEW, "run-operation", "apply_deletion_requests")
    assert applied.returncode == 0, applied.stdout[-4000:]
    again = dbt(work, AS_OF_NEW, "run-operation", "apply_deletion_requests")
    loaded = ingest(work, write_dump(clean_rows(), work / "kaggle"), AS_OF_NEW, "--next")
    assert loaded.returncode == 0, loaded.stdout + loaded.stderr
    dbt(work, AS_OF_NEW, "build", "--exclude", "package:elementary")
    outputs.append(dbt(work, AS_OF_NEW, "run-operation", "check_deletion_requests"))
    outputs += [applied, again]
    outputs.append(dbt(work, AS_OF_NEW, "run-operation", "check_deletion_requests", "--args", "{applied_only: true}"))
    return work, values, [f"{o.returncode}\n{o.stdout}" for o in outputs]


def test_check_fails_before_the_deletion(deleted: tuple[Path, set[str], list[str]]) -> None:
    before = deleted[2][0]
    assert before.startswith("1\n"), before[-2000:]
    assert '"marts"."fct_order_items"' in before and '"raw"."customers"' in before


def test_check_passes_after_the_deletion(deleted: tuple[Path, set[str], list[str]]) -> None:
    after = deleted[2][1]
    assert after.startswith("0\n"), after[-2000:]
    assert "none of the people in" in after


def test_apply_reports_the_people_and_a_second_apply_finds_nothing_pending(
    deleted: tuple[Path, set[str], list[str]],
) -> None:
    applied, again, applied_only = deleted[2][2:5]
    assert f"DELETIONS_APPLIED={len(person_values()[0])}" in applied, applied[-2000:]
    assert again.startswith("0\n") and "DELETIONS_APPLIED=0" in again, again[-2000:]
    assert "Deleted" not in again
    assert applied_only.startswith("0\n") and "none of the people in" in applied_only, applied_only[-2000:]
    with duckdb.connect(str(db_path(deleted[0])), read_only=True) as con:
        rows = con.execute("select applied_at is not null, people, rows_deleted > 0 from raw.deletion_log").fetchall()
    assert rows == [(True, len(person_values()[0]), True)]


def test_the_scan_finds_the_loaded_person_before(baseline: Path) -> None:
    found = tables_holding(db_path(baseline), person_values()[1])
    assert "raw.customers.customer_unique_id" in found
    assert "marts.fct_order_items.customer_unique_id" in found
    assert "marts.dim_customers.customer_key" in found


def test_no_table_holds_a_value_of_the_people(deleted: tuple[Path, set[str], list[str]]) -> None:
    work, values, _ = deleted
    assert tables_holding(db_path(work), values) == {}


def test_the_load_after_the_deletion_brings_everyone_else(deleted: tuple[Path, set[str], list[str]]) -> None:
    work, values, _ = deleted
    theirs = [o for o in clean_rows()["orders"] if o["order_id"] in values]
    assert theirs
    assert raw_orders(db_path(work)) == len(clean_rows()["orders"]) - len(theirs)

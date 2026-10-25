"""Load Olist CSVs from Kaggle into schema/dataset `raw` (`--dataset`) with dlt.

Destination: DuckDB (default, local dev), BigQuery (`--destination bigquery`) or
Databricks (`--destination databricks`).
BigQuery auth = Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS).
Databricks auth = DATABRICKS_HOST, DATABRICKS_HTTP_PATH (SQL warehouse) and either
DATABRICKS_TOKEN (personal access token) or DATABRICKS_CLIENT_ID +
DATABRICKS_CLIENT_SECRET (service principal, CI); tables go to catalog
DATABRICKS_CATALOG (default `olist`).

All source columns are loaded as strings: the raw layer mirrors the source exactly
(e.g. zip prefixes keep leading zeros). Typing happens in dbt staging.
The category translation file is a dbt seed, not loaded here.

Simulated arrival in monthly chunks (the Kaggle export is a static 2016-2018 dump):
- Order tables (orders + their items, payments, reviews) load by purchase month,
  `merge` on primary keys. The last loaded month is kept in dlt state, so each run
  loads only months after it, up to `--until YYYY-MM` (default: everything), or
  just the next month that has orders (`--next`: the scheduled Airflow "feed";
  once the dump is used up it loads no orders). When the cursor shows no new month
  in the dump, any mode but `--full-refresh` skips the load and only reads the
  status: an idle hourly run or a deploy then costs one warehouse query (plus the
  state read on a fresh runner) instead of a dozen.
- Reference tables (customers, products, sellers, geolocation) are full snapshots,
  `replace` on every run.
- Every row gets `_loaded_at` (UTC, one value per run): the cursor for incremental
  dbt models and the field for `dbt source freshness`. An order and its items,
  payments and reviews always load in the same chunk, so they share it.
- On BigQuery the order tables are partitioned by day of `_loaded_at`, so an
  incremental dbt run reads only the partitions since its cursor (D70). dlt sets
  partitioning only when it creates a table; existing tables are migrated by hand.

Examples:
    python ingestion/pipeline.py --until 2017-06   # first chunk: up to June 2017
    python ingestion/pipeline.py --until 2017-07   # loads July 2017 only
    python ingestion/pipeline.py --next            # loads August 2017 only
    python ingestion/pipeline.py                   # loads the rest
    python ingestion/pipeline.py --full-refresh    # drop tables + state, reload

A new column in a source file is stored in raw (dlt's default schema contract,
`evolve`); staging lists its columns, so nothing above raw changes until a PR adds
it. The run reports such columns as `new_columns` for the SCHEMA email (D61). A column
missing from a source file stays in raw and gets nulls; every load reports it as
`removed_columns` until the file has it again or the column is dropped.

Airflow (airflow/dags/olist_elt.py) passes `--duckdb-path` / `--dataset` to keep its
runs apart from local dev and prod. The last output line is the feed status as JSON
(`orders_loaded`, `orders_total`, `last_order_at`, `last_loaded_at`, `new_columns`,
`removed_columns`):
Airflow reads it to skip dbt when nothing is new and for the email reports, CD for
the SCHEMA email.
"""

import argparse
import csv
import functools
import json
import os
import re
import warnings
from datetime import UTC, datetime
from pathlib import Path

import dlt
import kagglehub
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv
from dlt.common.configuration.specs import GcpServiceAccountCredentials
from dlt.common.pipeline import LoadInfo
from dlt.common.schema import Schema
from dlt.destinations.adapters import bigquery_adapter

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = ROOT / "data" / "olist.duckdb"
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
BQ_LOCATION = "EU"
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "olist")
STATE_KEY = "orders_loaded_until"  # last loaded purchase month, "YYYY-MM"
# Olist purchase time, "2017-10-02 10:56:33" (optionally ISO "T", fractions)
TIMESTAMP = r"\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(\.\d+)?"

# The BigQuery client suggests the Storage API for fast reads; dlt here only writes
warnings.filterwarnings("ignore", message="Cannot create BigQuery Storage client")

# Reference tables: dlt table name -> source file
REFERENCE_TABLES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}

# Order tables: dlt table name -> (source file, primary key). `orders` is the driver.
ORDER_TABLES = {
    "orders": ("olist_orders_dataset.csv", ["order_id"]),
    "order_items": ("olist_order_items_dataset.csv", ["order_id", "order_item_id"]),
    "order_payments": ("olist_order_payments_dataset.csv", ["order_id", "payment_sequential"]),
    "order_reviews": ("olist_order_reviews_dataset.csv", ["review_id", "order_id"]),
}


def csv_header(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        return next(csv.reader(f))


def read_csv_as_strings(path: Path) -> pa.Table:
    header = csv_header(path)
    return pacsv.read_csv(
        path,
        convert_options=pacsv.ConvertOptions(
            column_types={name: pa.string() for name in header},
            strings_can_be_null=True,
        ),
    )


def with_loaded_at(table: pa.Table, loaded_at: datetime) -> pa.Table:
    # Time-zone aware: TIMESTAMPTZ on DuckDB, TIMESTAMP on BigQuery (both = an instant)
    column = pa.array([loaded_at] * table.num_rows, type=pa.timestamp("us", tz="UTC"))
    return table.append_column("_loaded_at", column)


def with_required(table: pa.Table, columns: list[str]) -> pa.Table:
    """Mark primary key columns non-nullable, matching the hint dlt derives from `primary_key`."""
    schema = pa.schema(
        field.with_nullable(False) if field.name in columns else field for field in table.schema
    )
    return table.cast(schema)


def read_orders(source_dir: Path) -> pa.Table:
    """The orders file, refusing malformed purchase times.

    The chunk and the cursor come from string order and the "YYYY-MM" prefix: one
    malformed value would pick the wrong month or poison the cursor.
    """
    orders = read_csv_as_strings(source_dir / ORDER_TABLES["orders"][0])
    purchased_at = pc.field("order_purchase_timestamp")
    bad = orders.filter(
        pc.is_valid(purchased_at) & ~pc.match_substring_regex(purchased_at, f"^{TIMESTAMP}$")
    )
    if bad.num_rows:
        examples = list(zip(bad["order_id"].to_pylist()[:3], bad["order_purchase_timestamp"].to_pylist()[:3]))
        raise ValueError(f"{bad.num_rows} orders with a malformed order_purchase_timestamp, e.g. {examples}")
    return orders


def next_month(month: str) -> str:
    """'2017-12' -> '2018-01'."""
    year, mon = map(int, month.split("-"))
    return f"{year + mon // 12:04d}-{mon % 12 + 1:02d}"


def month_key(value: str) -> str:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}")
    return value


@dlt.source(name="olist")
def olist_source(
    source_dir: Path, until: str | None = None, next_only: bool = False, partitioned: bool = False
):
    loaded_at = datetime.now(UTC)

    for table_name, file_name in REFERENCE_TABLES.items():
        table = read_csv_as_strings(source_dir / file_name)
        # `replace`: an empty file would wipe the table, and the sellers snapshot would
        # close every seller's history (hard_deletes: new_record)
        if not table.num_rows:
            raise ValueError(f"{file_name} has no rows: refusing to replace {table_name}")
        yield dlt.resource(
            [with_loaded_at(table, loaded_at)],
            name=table_name,
            write_disposition="replace",
        )

    # Evaluated lazily, inside extract: state is read after `refresh` has reset it
    @functools.cache
    def chunk_order_ids() -> pa.ChunkedArray:
        """Order ids with purchase month in (loaded_until, until]; advances the state."""
        state = dlt.current.source_state()
        loaded_until = state.get(STATE_KEY)
        # Timestamps are ISO strings, so string order = time order
        purchased_at = pc.field("order_purchase_timestamp")
        all_orders = read_orders(source_dir)
        in_chunk = pc.is_valid(purchased_at)
        if loaded_until:
            in_chunk &= purchased_at >= next_month(loaded_until)
        orders = all_orders.filter(in_chunk)
        # `--next`: the first month after the cursor that has orders. Not simply the
        # next calendar month: the dump has empty months (2016-11), which would
        # never move the cursor.
        chunk_until = until
        if next_only and orders.num_rows:
            chunk_until = pc.min(orders["order_purchase_timestamp"]).as_py()[:7]
        if chunk_until:
            orders = orders.filter(purchased_at < next_month(chunk_until))
        if orders.num_rows:
            state[STATE_KEY] = pc.max(orders["order_purchase_timestamp"]).as_py()[:7]
        print(
            f"Order chunk ({loaded_until or 'start'}, {chunk_until or 'end'}]: "
            f"{orders.num_rows} orders, state now {state.get(STATE_KEY)}"
        )
        return orders["order_id"]

    def order_rows(file_name: str, primary_key: list[str]):
        table = read_csv_as_strings(source_dir / file_name)
        table = table.filter(pc.is_in(table["order_id"], value_set=chunk_order_ids()))
        yield with_loaded_at(with_required(table, primary_key), loaded_at)

    for table_name, (file_name, primary_key) in ORDER_TABLES.items():
        resource = dlt.resource(
            order_rows(file_name, primary_key),
            name=table_name,
            write_disposition="merge",
            primary_key=primary_key,
        )
        if partitioned:
            # PARTITION BY DATE(_loaded_at): day, not hour, stays within BigQuery's
            # 4,000 partitions per table for about 11 years of daily loads
            bigquery_adapter(resource, partition="_loaded_at")
        yield resource


def loaded_until(pipeline: dlt.Pipeline) -> str | None:
    return pipeline.state.get("sources", {}).get("olist", {}).get(STATE_KEY)


def nothing_to_load(pipeline: dlt.Pipeline, source_dir: Path, until: str | None) -> bool:
    """True when the dump has no order month after the cursor (up to `until`).

    Decided before the load: a dlt run reads its state from the destination, then
    replaces the reference tables and runs four merges even for a chunk of 0 orders,
    and every one is a billed query (BigQuery bills at least 10 MB each). The cursor
    comes from the local dlt state; without one (fresh runner or volume) it is read
    from the destination first, the same restore a run would do, but only that.
    Anything unclear means "load as usual": no cursor anywhere (first load) or
    packages a failed run left behind (only a load retries them). Reference tables
    are not reloaded on a skipped run; in the dump they never change.
    """
    if pipeline.has_pending_data:
        return False
    if not loaded_until(pipeline):
        pipeline.sync_destination()
    cursor = loaded_until(pipeline)
    if not cursor:
        return False
    purchased_at = pc.field("order_purchase_timestamp")
    left = purchased_at >= next_month(cursor)
    if until:
        left &= purchased_at < next_month(until)
    return read_orders(source_dir).filter(left).num_rows == 0


def new_columns(load_info: LoadInfo, schema: Schema) -> dict[str, list[str]]:
    """Columns this load added to raw tables that existed before it: table -> names.

    From each load package's schema update (what the load changed in dlt's schema,
    restored from the destination on a fresh runner). A table the load created
    (first load, `--full-refresh`) comes with all its columns and is not a change.
    dlt's own tables and columns are skipped.
    """
    added: dict[str, list[str]] = {}
    for package in load_info.load_packages:
        for table, update in package.schema_update.items():
            if table.startswith("_dlt"):
                continue
            columns = [c for c in update.get("columns", {}) if not c.startswith("_dlt")]
            existing = [c for c in schema.tables[table].get("columns", {}) if not c.startswith("_dlt")]
            if columns and len(columns) < len(existing):
                added.setdefault(table, []).extend(columns)
    return added


def removed_columns(source_dir: Path, schema: Schema) -> dict[str, list[str]]:
    """Raw columns the source file no longer has: table -> names.

    dlt keeps such a column and loads nulls into it, so it is reported on every load
    while the file lacks it, not only on the first: no extra state, and the problem
    stays visible until it is fixed. Header names go through dlt's naming convention
    first, as dlt does when it loads them. `_loaded_at` (added here) and dlt's own
    columns are not in the file.
    """
    files = {**REFERENCE_TABLES, **{table: file for table, (file, _) in ORDER_TABLES.items()}}
    removed: dict[str, list[str]] = {}
    for table, file_name in files.items():
        if table not in schema.tables:
            continue
        header = {schema.naming.normalize_identifier(name) for name in csv_header(source_dir / file_name)}
        missing = [
            column
            for column in schema.tables[table].get("columns", {})
            if column != "_loaded_at" and not column.startswith("_dlt") and column not in header
        ]
        if missing:
            removed[table] = missing
    return removed


def feed_status(
    pipeline: dlt.Pipeline,
    orders_loaded: int,
    added_columns: dict[str, list[str]],
    dropped_columns: dict[str, list[str]],
) -> dict[str, object]:
    """This run's order count and column changes + the raw orders table as it is now.

    `last_loaded_at` only moves when a run brings orders (merge of 0 rows writes
    nothing): the same signal as `dbt source freshness`.
    """
    with pipeline.sql_client() as client:
        orders = client.make_qualified_table_name("orders")
        rows = client.execute_sql(
            f"select count(*), max(order_purchase_timestamp), max(_loaded_at) from {orders}"
        )
    assert rows, "no result from the orders status query"
    total, last_order_at, last_loaded_at = rows[0]
    # Airflow renders it into a dbt command line (Elementary's clock): a value from
    # the dump that isn't a plain timestamp must stop the run, not reach a shell
    if last_order_at is not None and not re.fullmatch(TIMESTAMP, last_order_at):
        raise ValueError(f"max order_purchase_timestamp is not a timestamp: {last_order_at!r}")
    return {
        "orders_loaded": orders_loaded,
        "orders_total": total,
        "last_order_at": last_order_at,
        "last_loaded_at": last_loaded_at.isoformat() if last_loaded_at else None,
        "new_columns": added_columns,
        "removed_columns": dropped_columns,
    }


def make_destination(name: str, duckdb_path: Path):
    if name == "bigquery":
        # No key given: dlt falls back to Application Default Credentials
        credentials = GcpServiceAccountCredentials()
        credentials.project_id = GCP_PROJECT_ID
        return dlt.destinations.bigquery(credentials=credentials, location=BQ_LOCATION)
    if name == "databricks":
        # Files are staged in a Unity Catalog volume dlt manages in the dataset's schema
        credentials = {
            "server_hostname": os.environ["DATABRICKS_HOST"],
            "http_path": os.environ["DATABRICKS_HTTP_PATH"],
            "catalog": DATABRICKS_CATALOG,
        }
        if os.environ.get("DATABRICKS_CLIENT_ID"):
            credentials["client_id"] = os.environ["DATABRICKS_CLIENT_ID"]
            credentials["client_secret"] = os.environ["DATABRICKS_CLIENT_SECRET"]
        else:
            credentials["access_token"] = os.environ["DATABRICKS_TOKEN"]
        return dlt.destinations.databricks(credentials=credentials)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return dlt.destinations.duckdb(str(duckdb_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--destination", choices=["duckdb", "bigquery", "databricks"], default="duckdb")
    chunk = parser.add_mutually_exclusive_group()
    chunk.add_argument(
        "--until", type=month_key, help="load order months up to this one, YYYY-MM (default: all)"
    )
    chunk.add_argument(
        "--next", action="store_true", help="load only the next month that has orders"
    )
    parser.add_argument(
        "--dataset", default="raw", help="target schema (DuckDB, Databricks) / dataset (BigQuery)"
    )
    parser.add_argument(
        "--duckdb-path", type=Path, default=DUCKDB_PATH, help="DuckDB file (default: %(default)s)"
    )
    parser.add_argument(
        "--full-refresh", action="store_true", help="drop raw tables and dlt state first"
    )
    args = parser.parse_args()

    source_dir = Path(kagglehub.dataset_download(KAGGLE_DATASET))
    pipeline = dlt.pipeline(
        # Separate state per destination and dataset. Not per DuckDB file: Airflow's
        # file has its own dlt state dir (container volume), so names don't clash.
        pipeline_name=f"olist_{args.destination}"
        + ("" if args.dataset == "raw" else f"_{args.dataset}"),
        destination=make_destination(args.destination, args.duckdb_path),
        dataset_name=args.dataset,
    )
    if not args.full_refresh and nothing_to_load(pipeline, source_dir, args.until):
        print(f"No new order months after {loaded_until(pipeline)}: load skipped")
        orders_loaded, added_columns, dropped_columns = 0, {}, {}
    else:
        source = olist_source(
            source_dir,
            until=args.until,
            next_only=args.next,
            partitioned=args.destination == "bigquery",
        )
        if args.full_refresh:
            load_info = pipeline.run(source, refresh="drop_sources")
        else:
            load_info = pipeline.run(source)
        print(load_info)
        normalize_info = pipeline.last_trace.last_normalize_info if pipeline.last_trace else None
        assert normalize_info is not None, "pipeline.run() finished without a normalize step"
        orders_loaded = normalize_info.row_counts.get("orders", 0)
        added_columns = new_columns(load_info, pipeline.default_schema)
        for table, columns in added_columns.items():
            print(f"New source columns in {table}: {', '.join(columns)} (stored in raw only)")
        dropped_columns = removed_columns(source_dir, pipeline.default_schema)
        for table, columns in dropped_columns.items():
            print(f"Source columns missing in {table}: {', '.join(columns)} (loaded as nulls)")
    # Last line on purpose: Airflow's BashOperator pushes it as the task's XCom
    print(json.dumps(feed_status(pipeline, orders_loaded, added_columns, dropped_columns)))


if __name__ == "__main__":
    main()

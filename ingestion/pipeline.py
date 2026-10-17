"""Load Olist CSVs from Kaggle into schema/dataset `raw` (`--dataset`) with dlt.

Destination: DuckDB (default, local dev) or BigQuery (`--destination bigquery`).
BigQuery auth = Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS).

All source columns are loaded as strings: the raw layer mirrors the source exactly
(e.g. zip prefixes keep leading zeros). Typing happens in dbt staging.
The category translation file is a dbt seed, not loaded here.

Simulated arrival in monthly chunks (the Kaggle export is a static 2016-2018 dump):
- Order tables (orders + their items, payments, reviews) load by purchase month,
  `merge` on primary keys. The last loaded month is kept in dlt state, so each run
  loads only months after it, up to `--until YYYY-MM` (default: everything), or
  just the next month that has orders (`--next`: the scheduled Airflow "feed";
  once the dump is used up it loads no orders).
- Reference tables (customers, products, sellers, geolocation) are full snapshots,
  `replace` on every run.
- Every row gets `_loaded_at` (UTC, one value per run): the cursor for incremental
  dbt models and the field for `dbt source freshness`.

Examples:
    python ingestion/pipeline.py --until 2017-06   # first chunk: up to June 2017
    python ingestion/pipeline.py --until 2017-07   # loads July 2017 only
    python ingestion/pipeline.py --next            # loads August 2017 only
    python ingestion/pipeline.py                   # loads the rest
    python ingestion/pipeline.py --full-refresh    # drop tables + state, reload

Airflow (airflow/dags/olist_elt.py) passes `--duckdb-path` / `--dataset` to keep its
runs apart from local dev and prod. The last output line is the feed status as JSON
(`orders_loaded`, `orders_total`, `last_order_at`, `last_loaded_at`): Airflow reads it
to skip dbt when nothing is new and for the email report.
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

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = ROOT / "data" / "olist.duckdb"
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
BQ_LOCATION = "EU"
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


def read_csv_as_strings(path: Path) -> pa.Table:
    with path.open(newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
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


def next_month(month: str) -> str:
    """'2017-12' -> '2018-01'."""
    year, mon = map(int, month.split("-"))
    return f"{year + mon // 12:04d}-{mon % 12 + 1:02d}"


def month_key(value: str) -> str:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}")
    return value


@dlt.source(name="olist")
def olist_source(source_dir: Path, until: str | None = None, next_only: bool = False):
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
        all_orders = read_csv_as_strings(source_dir / ORDER_TABLES["orders"][0])
        # The chunk and the cursor come from string order and the "YYYY-MM" prefix:
        # one malformed value would pick the wrong month or poison the cursor
        bad = all_orders.filter(
            pc.is_valid(purchased_at) & ~pc.match_substring_regex(purchased_at, f"^{TIMESTAMP}$")
        )
        if bad.num_rows:
            examples = list(zip(bad["order_id"].to_pylist()[:3], bad["order_purchase_timestamp"].to_pylist()[:3]))
            raise ValueError(f"{bad.num_rows} orders with a malformed order_purchase_timestamp, e.g. {examples}")
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
        yield dlt.resource(
            order_rows(file_name, primary_key),
            name=table_name,
            write_disposition="merge",
            primary_key=primary_key,
        )


def feed_status(pipeline: dlt.Pipeline) -> dict[str, object]:
    """This run's order count + the raw orders table as it is now (after the load).

    `last_loaded_at` only moves when a run brings orders (merge of 0 rows writes
    nothing): the same signal as `dbt source freshness`.
    """
    trace = pipeline.last_trace
    normalize_info = trace.last_normalize_info if trace else None
    assert normalize_info is not None, "pipeline.run() finished without a normalize step"
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
        "orders_loaded": normalize_info.row_counts.get("orders", 0),
        "orders_total": total,
        "last_order_at": last_order_at,
        "last_loaded_at": last_loaded_at.isoformat() if last_loaded_at else None,
    }


def make_destination(name: str, duckdb_path: Path):
    if name == "bigquery":
        # No key given: dlt falls back to Application Default Credentials
        credentials = GcpServiceAccountCredentials()
        credentials.project_id = GCP_PROJECT_ID
        return dlt.destinations.bigquery(credentials=credentials, location=BQ_LOCATION)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return dlt.destinations.duckdb(str(duckdb_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--destination", choices=["duckdb", "bigquery"], default="duckdb")
    chunk = parser.add_mutually_exclusive_group()
    chunk.add_argument(
        "--until", type=month_key, help="load order months up to this one, YYYY-MM (default: all)"
    )
    chunk.add_argument(
        "--next", action="store_true", help="load only the next month that has orders"
    )
    parser.add_argument(
        "--dataset", default="raw", help="target schema (DuckDB) / dataset (BigQuery)"
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
    source = olist_source(source_dir, until=args.until, next_only=args.next)
    if args.full_refresh:
        load_info = pipeline.run(source, refresh="drop_sources")
    else:
        load_info = pipeline.run(source)
    print(load_info)
    # Last line on purpose: Airflow's BashOperator pushes it as the task's XCom
    print(json.dumps(feed_status(pipeline)))


if __name__ == "__main__":
    main()

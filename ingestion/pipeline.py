"""Load Olist CSVs from Kaggle into schema/dataset `raw` (`--dataset`) with dlt.

Destination: DuckDB (default, local dev), BigQuery (`--destination bigquery`),
Databricks (`--destination databricks`) or Snowflake (`--destination snowflake`).
BigQuery auth = Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS).
Databricks auth = DATABRICKS_HOST, DATABRICKS_HTTP_PATH (SQL warehouse) and either
DATABRICKS_TOKEN (personal access token) or DATABRICKS_CLIENT_ID +
DATABRICKS_CLIENT_SECRET (service principal, CI); tables go to catalog
DATABRICKS_CATALOG (default `olist`).
Snowflake auth = key pair: SNOWFLAKE_ACCOUNT (`<org>-<account>`), SNOWFLAKE_USER
(default `OLIST_SVC`) and SNOWFLAKE_PRIVATE_KEY_PATH (PEM file; CI writes its
secret to one); database / warehouse / role default to the
objects in infra/snowflake (SNOWFLAKE_DATABASE, _WAREHOUSE, _ROLE). dlt uppercases
identifiers there (RAW.ORDERS); dbt's unquoted names resolve to them.

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
  state read on a fresh runner) instead of a dozen. The local cursor is trusted only
  while the destination's raw orders end in the same month: a new, deleted or
  replaced DuckDB file (or a dropped dataset) resets it from the destination.
- Reference tables (customers, products, sellers, geolocation) are full snapshots,
  `replace` on every run.
- Every row gets `_loaded_at` (UTC, one value per run): the cursor for incremental
  dbt models and the field for `dbt source freshness`. An order and its items,
  payments and reviews always load in the same chunk, so they share it.
- On BigQuery the order tables are partitioned by day of `_loaded_at`, so an
  incremental dbt run reads only the partitions since its cursor (D70). dlt sets
  partitioning only when it creates a table; existing tables are migrated by hand.
- People with a deletion request (table `OLIST_DELETION_REQUESTS`, default
  `pii_keys.deletion_requests`, D105) are left out of every load: their customer
  rows, their orders and the orders' items, payments and reviews. Without the
  table everyone loads, except on the destinations `OLIST_DELETION_REQUESTS_REQUIRED`
  names (comma-separated): there the load fails before writing anything.
- After the dump (D112): with `--synthetic-every-hours H`, `--next` on a used-up
  dump serves a generated month (ingestion/synthetic.py) once the last load of
  orders is at least H hours old; the runs in between skip the load as before.
  `--synthetic-every-hours H --until YYYY-MM` generates every month up to it at once.
  Raw orders carry `_is_synthetic` (false for the dump's rows). Once the cursor is
  past the dump, every load keeps the generated customers and sellers of the loaded
  months in the reference snapshots, with or without the flag.

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
import hashlib
import json
import os
import re
import sys
import warnings
from datetime import UTC, datetime, timedelta
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
from dlt.destinations.exceptions import DatabaseUndefinedRelation

# Run as a script (Airflow, CI) or imported as ingestion.pipeline (tests)
sys.path.insert(0, str(Path(__file__).resolve().parent))
import synthetic  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = ROOT / "data" / "olist.duckdb"
# Review labels (sentiment, topic, English translation): CSV batches kept in the repo,
# not part of the dump (ingestion/review_labels/, D92)
LABELS_DIR = ROOT / "ingestion" / "review_labels" / "labels"
# Labels for reviews that arrive later (classify_api.py): at most this many new texts per run
LABEL_MAX_TEXTS = int(os.environ.get("OLIST_LABEL_MAX_TEXTS", "500"))
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
BQ_LOCATION = "EU"
DESTINATIONS = ("duckdb", "bigquery", "databricks", "snowflake")
DATABRICKS_CATALOG = os.environ.get("DATABRICKS_CATALOG", "olist")
STATE_KEY = "orders_loaded_until"  # last loaded purchase month, "YYYY-MM"
SYNTHETIC_VERSION_KEY = "synthetic_version"  # generator version of the loaded generated months
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
# Added to raw orders by the loader, not in the source files: true on generated orders
SYNTHETIC_FLAG = "_is_synthetic"
LOADER_COLUMNS = {"_loaded_at", SYNTHETIC_FLAG}
# A run this much before the interval is up still counts as due: the hourly schedule
# starts a few minutes late or early, and a strict check would slip an hour a day
SYNTHETIC_SLACK = timedelta(minutes=30)


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


SOURCE_FILES = {**REFERENCE_TABLES, **{table: file for table, (file, _) in ORDER_TABLES.items()}}


class SourceDump:
    """The source files as tables of strings, with generated months after them (D112).

    `synthetic_until` unset: the files as they are. Set: orders, their items, payments
    and reviews, customers and sellers also get every generated month after the
    dump up to that month. Orders always get `_is_synthetic`, false for the files' rows.
    """

    def __init__(self, source_dir: Path) -> None:
        self.source_dir = source_dir
        self.synthetic_until: str | None = None

    def header(self, table: str) -> list[str]:
        header = csv_header(self.source_dir / SOURCE_FILES[table])
        return [*header, SYNTHETIC_FLAG] if table == "orders" else header

    @functools.cached_property
    def real_orders(self) -> pa.Table:
        return read_orders(self.source_dir)

    @functools.cache
    def _real(self, table: str) -> pa.Table:
        """A file read once per run: the generator needs it as well as the load."""
        if table == "orders":
            return self.real_orders
        return read_csv_as_strings(self.source_dir / SOURCE_FILES[table])

    @functools.cached_property
    def last_real_month(self) -> str | None:
        last = pc.max(self.real_orders["order_purchase_timestamp"]).as_py()
        return last[:7] if last else None

    @functools.cache
    def _generate(self, until: str) -> dict[str, pa.Table]:
        real = synthetic.Dump({table: self._real(table) for table in synthetic.TABLES})
        generated = synthetic.generate(real, until)
        print(
            f"Generated months ({real.last_month}, {until}]: {generated['orders'].num_rows} orders, "
            f"{generated['sellers'].num_rows} new sellers"
        )
        return generated

    def table(self, name: str) -> pa.Table:
        until = self.synthetic_until
        if until and name in synthetic.TABLES:
            parts = [self._real(name), self._generate(until)[name]]
        elif name == "orders":
            parts = [self.real_orders]
        else:
            return read_csv_as_strings(self.source_dir / SOURCE_FILES[name])
        if name == "orders":
            parts = [
                part.append_column(SYNTHETIC_FLAG, pa.array([i > 0] * part.num_rows, pa.bool_()))
                for i, part in enumerate(parts)
            ]
        return pa.concat_tables(parts)

    def orders(self) -> pa.Table:
        return self.table("orders")


def refuse_conflicting_rows(table: pa.Table, primary_key: list[str], file_name: str) -> None:
    """Refuse two different rows with one key: dlt's merge would silently keep one.

    Identical rows pass, the merge collapses them and nothing is lost.
    """
    distinct_rows = table.group_by(table.column_names).aggregate([]).num_rows
    distinct_keys = table.group_by(primary_key).aggregate([]).num_rows
    if distinct_rows > distinct_keys:
        raise ValueError(f"{file_name}: {distinct_rows - distinct_keys} conflicting rows for one {primary_key}")


def next_month(month: str) -> str:
    """'2017-12' -> '2018-01'."""
    year, mon = map(int, month.split("-"))
    return f"{year + mon // 12:04d}-{mon % 12 + 1:02d}"


def month_key(value: str) -> str:
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", value):
        raise argparse.ArgumentTypeError(f"expected YYYY-MM, got {value!r}")
    return value


def deletion_requests_table(destination: str) -> str:
    """The table of deletion requests, qualified for the destination's SQL (D105).

    `OLIST_DELETION_REQUESTS` = `schema.table` (default `pii_keys.deletion_requests`),
    the same variable dbt's deletion macros read. On BigQuery the dataset sits in
    GCP_PROJECT_ID. Checked, since it is pasted into SQL.
    """
    name = os.environ.get("OLIST_DELETION_REQUESTS", "pii_keys.deletion_requests")
    if not re.fullmatch(r"[A-Za-z0-9_]+\.[A-Za-z0-9_]+", name):
        raise ValueError(f"OLIST_DELETION_REQUESTS must be schema.table, got {name!r}")
    return f"`{GCP_PROJECT_ID}.{name}`" if destination == "bigquery" else name


def deletion_requests_required(destination: str) -> bool:
    """Whether a load to `destination` must find the request table (D105).

    `OLIST_DELETION_REQUESTS_REQUIRED` = destinations, comma-separated (`bigquery`);
    unset = none. Set where a request exists: there a missing table means a dropped
    or misnamed one, and loading everyone would bring the deleted people back. An
    unknown name is an error, not "not required": a typo must not switch the check off.
    """
    names = {n.strip() for n in os.environ.get("OLIST_DELETION_REQUESTS_REQUIRED", "").split(",")} - {""}
    if unknown := names - set(DESTINATIONS):
        raise ValueError(
            f"OLIST_DELETION_REQUESTS_REQUIRED must list destinations ({', '.join(DESTINATIONS)}), "
            f"got {', '.join(sorted(unknown))!r}"
        )
    return destination in names


def read_deletion_requests(pipeline: dlt.Pipeline, destination: str) -> frozenset[str]:
    """The `customer_unique_id`s of every deletion request.

    Only DuckDB (local, Airflow's DuckDB DAG, tests) and BigQuery (prod, Airflow's
    BigQuery DAG, the BigQuery CI deletion job) have the table; a destination
    without it loads everyone, unless `deletion_requests_required` says the table
    must be there: then the load stops before anything is written.
    """
    table = deletion_requests_table(destination)
    required = deletion_requests_required(destination)
    try:
        with pipeline.sql_client() as client:
            rows = client.execute_sql(f"select distinct customer_unique_id from {table}")
    except DatabaseUndefinedRelation:
        if required:
            raise RuntimeError(
                f"Deletion request table {table} not found, and OLIST_DELETION_REQUESTS_REQUIRED "
                f"names {destination}: nothing loaded, since a load without the table would bring "
                "the deleted people back. Restore the table or fix OLIST_DELETION_REQUESTS."
            ) from None
        return frozenset()
    return frozenset(row[0] for row in rows or [])


def suppress_people(customers: pa.Table, people: frozenset[str]) -> tuple[pa.Table, pa.Array]:
    """Customers without the requested people, and the dropped rows' `customer_id`s.

    `customer_id` is per order (D94: one person, many ids), so the orders of the
    people follow from the ids, and the order tables from the orders.
    """
    if not people:
        return customers, pa.array([], pa.string())
    # is_in gives false, not null, for a null id: such rows stay
    requested = pc.field("customer_unique_id").isin(pa.array(sorted(people), pa.string()))
    return customers.filter(~requested), customers.filter(requested)["customer_id"].combine_chunks()


@dlt.source(name="olist")
def olist_source(
    dump: SourceDump,
    until: str | None = None,
    next_only: bool = False,
    partitioned: bool = False,
    deleted_people: frozenset[str] = frozenset(),
):
    """The dump as dlt resources, without the people of `deleted_people` (D105).

    A deletion request removes a person from raw once (dbt's `apply_deletion_requests`);
    every later load must leave them out too, or the next `replace` of customers and
    the next order month would bring them back.
    """
    loaded_at = datetime.now(UTC)
    customers, deleted_customer_ids = suppress_people(dump.table("customers"), deleted_people)
    if deleted_people:
        print(
            f"Deletion requests: {len(deleted_people)} people, "
            f"{len(deleted_customer_ids)} customer rows left out of this load"
        )

    for table_name, file_name in REFERENCE_TABLES.items():
        table = customers if table_name == "customers" else dump.table(table_name)
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
        all_orders = dump.orders()
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
        if dump.synthetic_until:
            state[SYNTHETIC_VERSION_KEY] = synthetic.VERSION
        print(
            f"Order chunk ({loaded_until or 'start'}, {chunk_until or 'end'}]: "
            f"{orders.num_rows} orders, state now {state.get(STATE_KEY)}"
        )
        # After the cursor moved: a month whose orders are all of deleted people is
        # still a loaded month
        if len(deleted_customer_ids):
            orders = orders.filter(~pc.field("customer_id").isin(deleted_customer_ids))
        return orders["order_id"]

    def order_rows(table_name: str, file_name: str, primary_key: list[str]):
        table = dump.table(table_name)
        table = table.filter(pc.is_in(table["order_id"], value_set=chunk_order_ids()))
        refuse_conflicting_rows(table, primary_key, file_name)
        yield with_loaded_at(with_required(table, primary_key), loaded_at)

    for table_name, (file_name, primary_key) in ORDER_TABLES.items():
        resource = dlt.resource(
            order_rows(table_name, file_name, primary_key),
            name=table_name,
            write_disposition="merge",
            primary_key=primary_key,
        )
        if partitioned:
            # PARTITION BY DATE(_loaded_at): day, not hour, stays within BigQuery's
            # 4,000 partitions per table for about 11 years of daily loads
            bigquery_adapter(resource, partition="_loaded_at")
        yield resource


def read_review_labels(labels_dir: Path) -> tuple[pa.Table, str]:
    """Every label batch as one table of strings, and a short sha of the files' content."""
    paths = sorted(labels_dir.glob("*.csv"))
    sha = hashlib.sha256()
    for path in paths:
        sha.update(path.name.encode())
        sha.update(path.read_bytes())
    tables = [
        pacsv.read_csv(
            path,
            parse_options=pacsv.ParseOptions(newlines_in_values=True),
            convert_options=pacsv.ConvertOptions(
                column_types={name: pa.string() for name in csv_header(path)},
                strings_can_be_null=True,
                # only an empty field is null: "N/A" is a real translation (of "N/D")
                null_values=[""],
            ),
        )
        for path in paths
    ]
    return pa.concat_tables(tables) if tables else pa.table({}), sha.hexdigest()[:16]


def loaded_labels_sha(pipeline: dlt.Pipeline) -> str | None:
    try:
        with pipeline.sql_client() as client:
            labels = client.make_qualified_table_name("review_labels")
            rows = client.execute_sql(f"select max(labels_sha) from {labels}")
    except DatabaseUndefinedRelation:
        return None
    return rows[0][0] if rows else None


def label_new_reviews(pipeline: dlt.Pipeline, labels_dir: Path) -> None:
    """Label review texts that have no label yet, as a new batch file next to the others.

    A repeated text gets its label copied; a text never seen goes to the Claude API, only
    with ANTHROPIC_API_KEY set (D92). On the static dump nothing is open: one query, no call.
    """
    import sys

    sys.path.insert(0, str(ROOT / "ingestion" / "review_labels"))
    import classify_api

    loaded = pipeline.default_schema.tables.get("order_reviews", {}).get("columns", {})
    if not {"review_comment_title", "review_comment_message"} <= loaded.keys():
        return  # nothing loaded yet, or a feed without review texts
    try:
        with pipeline.sql_client() as client:
            reviews = client.make_qualified_table_name("order_reviews")
            rows = client.execute_sql(
                f"""
                select distinct review_id, review_score,
                    coalesce(review_comment_title, ''), coalesce(review_comment_message, '')
                from {reviews}
                where coalesce(review_comment_title, '') <> '' or coalesce(review_comment_message, '') <> ''
                """
            )
    except DatabaseUndefinedRelation:
        return
    keys = ["review_id", "review_score", "review_comment_title", "review_comment_message"]
    texts = [dict(zip(keys, map(str, row), strict=True)) for row in rows or []]
    classify_api.label_new_reviews(texts, labels_dir, max_texts=LABEL_MAX_TEXTS)


def load_review_labels(pipeline: dlt.Pipeline, labels_dir: Path) -> int:
    """Replace raw.review_labels when the label files changed; returns the rows loaded.

    Not tied to the order months: labels change with the repo, not with the dump, and
    a run that skips the orders still has to bring new labels (prod deploys, Airflow).
    The files' sha is stored in every row, so an unchanged repo costs one query.
    """
    table, sha = read_review_labels(labels_dir)
    if not table.num_rows:
        print("No review label files: review_labels not loaded")
        return 0
    if loaded_labels_sha(pipeline) == sha:
        print(f"Review labels unchanged ({sha}): load skipped")
        return 0
    table = table.append_column("labels_sha", pa.array([sha] * table.num_rows, type=pa.string()))
    table = with_loaded_at(with_required(table, ["review_id"]), datetime.now(UTC))
    pipeline.run(
        dlt.resource([table], name="review_labels", write_disposition="replace", primary_key="review_id")
    )
    print(f"Review labels {sha}: {table.num_rows} rows loaded")
    return table.num_rows


def loaded_until(pipeline: dlt.Pipeline) -> str | None:
    return pipeline.state.get("sources", {}).get("olist", {}).get(STATE_KEY)


OrdersStatus = tuple[int, str | None, datetime | None]


def orders_status(pipeline: dlt.Pipeline) -> OrdersStatus | None:
    """Count, last purchase time and last `_loaded_at` of raw orders; None without the table."""
    try:
        with pipeline.sql_client() as client:
            orders = client.make_qualified_table_name("orders")
            rows = client.execute_sql(
                f"select count(*), max(order_purchase_timestamp), max(_loaded_at) from {orders}"
            )
    except DatabaseUndefinedRelation:
        return None
    assert rows, "no result from the orders status query"
    total, last_order_at, last_loaded_at = rows[0]
    return total, last_order_at, last_loaded_at


def restore_cursor(pipeline: dlt.Pipeline, status: OrdersStatus | None) -> str | None:
    """The last loaded purchase month, local state checked against the destination.

    The cursor comes from the local dlt state when it matches the last purchase month
    of raw orders (`status`, the query the feed status needs anyway). Otherwise it is
    read from the destination, the same restore a run would do, but only that: no
    local cursor (fresh runner or volume), or one the destination doesn't have (a
    new, deleted or replaced DuckDB file, a dropped dataset; the pipeline name is
    not per file). Such a local state is dropped first: dlt keeps a local state that
    is newer than the destination's, and the load would then start after its cursor.
    With packages a failed run left behind the local state stays as it is: only a
    load retries them.
    """
    if pipeline.has_pending_data:
        return loaded_until(pipeline)
    local_cursor = loaded_until(pipeline)
    last_order_at = status[1] if status else None
    if local_cursor != (last_order_at[:7] if last_order_at else None):
        if local_cursor:
            print(
                f"Local dlt state has orders until {local_cursor}, the destination "
                f"until {last_order_at[:7] if last_order_at else 'none'}: state reset from the destination"
            )
            pipeline.drop()
        pipeline.sync_destination()
    return loaded_until(pipeline)


def check_synthetic_version(pipeline: dlt.Pipeline, cursor: str | None, last_real_month: str | None) -> None:
    """Refuse a load onto generated months of another generator version (D112).

    The loader regenerates every loaded month for the reference snapshots: months of
    another version would come out different, and their orders would lose their
    customers and sellers. A full refresh reloads the dump and generates anew.
    """
    if not (cursor and last_real_month and cursor > last_real_month):
        return
    loaded = pipeline.state.get("sources", {}).get("olist", {}).get(SYNTHETIC_VERSION_KEY)
    if loaded != synthetic.VERSION:
        raise SystemExit(
            f"Raw holds generated months up to {cursor} of generator version {loaded}, the generator "
            f"is version {synthetic.VERSION}: run once with --full-refresh (Airflow: param full_refresh)"
        )


def synthetic_until(
    last_real_month: str | None,
    cursor: str | None,
    until: str | None,
    next_only: bool,
    every_hours: float | None,
    last_loaded_at: datetime | None,
    now: datetime,
) -> str | None:
    """The last generated month this run's dump needs; None: the files alone (D112).

    A cursor past the dump keeps its generated months, so that the reference
    snapshots (replaced on every load) keep their generated customers and sellers.
    With `every_hours`: `--until` past the dump generates up to it; `--next` on a
    used-up dump adds the month after the cursor once the last load of orders is
    `every_hours` old (minus SYNTHETIC_SLACK).
    """
    if not last_real_month:
        return None
    months = [cursor] if cursor and cursor > last_real_month else []
    if every_hours is not None:
        if until and until > last_real_month:
            months.append(until)
        elif next_only and cursor and cursor >= last_real_month:
            if last_loaded_at and last_loaded_at.tzinfo is None:
                last_loaded_at = last_loaded_at.replace(tzinfo=UTC)
            due = last_loaded_at is None or now - last_loaded_at >= timedelta(hours=every_hours) - SYNTHETIC_SLACK
            if due:
                months.append(next_month(cursor))
            else:
                print(f"Next generated month due {every_hours:g} h after the last load ({last_loaded_at})")
    return max(months) if months else None


def nothing_to_load(pipeline: dlt.Pipeline, dump: SourceDump, until: str | None, cursor: str | None) -> bool:
    """True when the dump has no order month after the cursor (up to `until`).

    Decided before the load: a dlt run reads its state from the destination, then
    replaces the reference tables and runs four merges even for a chunk of 0 orders,
    and every one is a billed query (BigQuery bills at least 10 MB each). The cursor
    is restore_cursor()'s. Anything unclear means "load as usual": no cursor anywhere
    (first load) or packages a failed run left behind (only a load retries them).
    Reference tables are not reloaded on a skipped run; in the dump they never change.
    """
    if pipeline.has_pending_data or not cursor:
        return False
    purchased_at = pc.field("order_purchase_timestamp")
    left = purchased_at >= next_month(cursor)
    if until:
        left &= purchased_at < next_month(until)
    return dump.orders().filter(left).num_rows == 0


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
            # The loader's own columns are not source changes (D112: `_is_synthetic`
            # reaches an existing raw table with its first load after the change)
            source_columns = [c for c in schema.tables[table].get("columns", {}) if not c.startswith("_dlt")]
            columns = [c for c in update.get("columns", {}) if c in source_columns and c not in LOADER_COLUMNS]
            existing = [c for c in source_columns if c not in LOADER_COLUMNS]
            if columns and len(columns) < len(existing):
                added.setdefault(table, []).extend(columns)
    return added


def removed_columns(dump: SourceDump, schema: Schema) -> dict[str, list[str]]:
    """Raw columns the source file no longer has: table -> names.

    dlt keeps such a column and loads nulls into it, so it is reported on every load
    while the file lacks it, not only on the first: no extra state, and the problem
    stays visible until it is fixed. Header names go through dlt's naming convention
    first, as dlt does when it loads them. The loader's columns (`_loaded_at`,
    `_is_synthetic`) and dlt's own are not in the file.
    """
    removed: dict[str, list[str]] = {}
    for table in SOURCE_FILES:
        if table not in schema.tables:
            continue
        header = {schema.naming.normalize_identifier(name) for name in dump.header(table)}
        missing = [
            column
            for column in schema.tables[table].get("columns", {})
            if column not in LOADER_COLUMNS and not column.startswith("_dlt") and column not in header
        ]
        if missing:
            removed[table] = missing
    return removed


def feed_status(
    status: OrdersStatus | None,
    orders_loaded: int,
    added_columns: dict[str, list[str]],
    dropped_columns: dict[str, list[str]],
    labels_loaded: int = 0,
) -> dict[str, object]:
    """This run's order count and column changes + the raw orders table as it is now.

    `last_loaded_at` only moves when a run brings orders (merge of 0 rows writes
    nothing): the same signal as `dbt source freshness`.
    """
    assert status is not None, "no raw orders table after the run"
    total, last_order_at, last_loaded_at = status
    # Airflow renders it into a dbt command line (Elementary's clock): a value from
    # the dump that isn't a plain timestamp must stop the run, not reach a shell
    if last_order_at is not None and not re.fullmatch(TIMESTAMP, last_order_at):
        raise ValueError(f"max order_purchase_timestamp is not a timestamp: {last_order_at!r}")
    return {
        "orders_loaded": orders_loaded,
        # New review labels are new data too: Airflow and the prod deploy rebuild for them
        "labels_loaded": labels_loaded,
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
    if name == "snowflake":
        # Files go through each table's internal stage (PUT + COPY INTO)
        credentials = {
            "host": os.environ["SNOWFLAKE_ACCOUNT"],
            "username": os.environ.get("SNOWFLAKE_USER", "OLIST_SVC"),
            "database": os.environ.get("SNOWFLAKE_DATABASE", "OLIST"),
            "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
            "role": os.environ.get("SNOWFLAKE_ROLE", "OLIST_RUNNER"),
            "private_key_path": os.path.expanduser(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]),
        }
        return dlt.destinations.snowflake(credentials=credentials)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    return dlt.destinations.duckdb(str(duckdb_path))


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--destination", choices=DESTINATIONS, default="duckdb")
    chunk = parser.add_mutually_exclusive_group()
    chunk.add_argument(
        "--until", type=month_key, help="load order months up to this one, YYYY-MM (default: all)"
    )
    chunk.add_argument(
        "--next", action="store_true", help="load only the next month that has orders"
    )
    parser.add_argument(
        "--dataset", default="raw", help="target schema (DuckDB, Databricks, Snowflake) / dataset (BigQuery)"
    )
    parser.add_argument(
        "--duckdb-path", type=Path, default=DUCKDB_PATH, help="DuckDB file (default: %(default)s)"
    )
    parser.add_argument(
        "--full-refresh", action="store_true", help="drop raw tables and dlt state first"
    )
    parser.add_argument(
        "--synthetic-every-hours",
        type=float,
        metavar="H",
        help="after the dump: --next loads a generated month once the last load is H hours old (D112)",
    )
    args = parser.parse_args()

    dump = SourceDump(Path(kagglehub.dataset_download(KAGGLE_DATASET)))
    pipeline = dlt.pipeline(
        # Separate state per destination and dataset. Not per DuckDB file: Airflow's
        # file has its own dlt state dir (container volume); a local cursor from
        # another file is caught by nothing_to_load().
        pipeline_name=f"olist_{args.destination}"
        + ("" if args.dataset == "raw" else f"_{args.dataset}"),
        destination=make_destination(args.destination, args.duckdb_path),
        dataset_name=args.dataset,
    )
    status = None if args.full_refresh else orders_status(pipeline)
    cursor = None if args.full_refresh else restore_cursor(pipeline, status)
    check_synthetic_version(pipeline, cursor, dump.last_real_month)
    dump.synthetic_until = synthetic_until(
        dump.last_real_month,
        cursor,
        args.until,
        args.next,
        args.synthetic_every_hours,
        status[2] if status else None,
        datetime.now(UTC),
    )
    if not args.full_refresh and nothing_to_load(pipeline, dump, args.until, cursor):
        print(f"No new order months after {loaded_until(pipeline)}: load skipped")
        orders_loaded, added_columns, dropped_columns = 0, {}, {}
    else:
        source = olist_source(
            dump,
            until=args.until,
            next_only=args.next,
            partitioned=args.destination == "bigquery",
            deleted_people=read_deletion_requests(pipeline, args.destination),
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
        dropped_columns = removed_columns(dump, pipeline.default_schema)
        for table, columns in dropped_columns.items():
            print(f"Source columns missing in {table}: {', '.join(columns)} (loaded as nulls)")
        status = orders_status(pipeline)
    label_new_reviews(pipeline, LABELS_DIR)
    labels_loaded = load_review_labels(pipeline, LABELS_DIR)
    # Last line on purpose: Airflow's BashOperator pushes it as the task's XCom
    print(json.dumps(feed_status(status, orders_loaded, added_columns, dropped_columns, labels_loaded)))


if __name__ == "__main__":
    main()

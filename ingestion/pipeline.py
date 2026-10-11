"""Load Olist CSVs from Kaggle into schema/dataset `raw` with dlt.

Destination: DuckDB (default, local dev) or BigQuery (`--destination bigquery`).
BigQuery auth = Application Default Credentials (GOOGLE_APPLICATION_CREDENTIALS).

All columns are loaded as strings: the raw layer mirrors the source exactly
(e.g. zip prefixes keep leading zeros). Typing happens in dbt staging.
The category translation file is a dbt seed, not loaded here.
"""

import argparse
import csv
import os
from pathlib import Path

import dlt
import kagglehub
import pyarrow as pa
import pyarrow.csv as pacsv

ROOT = Path(__file__).resolve().parents[1]
DUCKDB_PATH = ROOT / "data" / "olist.duckdb"
KAGGLE_DATASET = "olistbr/brazilian-ecommerce"
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
BQ_LOCATION = "EU"

# dlt table name -> source file
TABLES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
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


@dlt.source(name="olist")
def olist_source(source_dir: Path):
    for table_name, file_name in TABLES.items():
        yield dlt.resource(
            read_csv_as_strings(source_dir / file_name),
            name=table_name,
            write_disposition="replace",
        )


def make_destination(name: str):
    if name == "bigquery":
        return dlt.destinations.bigquery(
            credentials={"project_id": GCP_PROJECT_ID}, location=BQ_LOCATION
        )
    DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return dlt.destinations.duckdb(str(DUCKDB_PATH))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--destination", choices=["duckdb", "bigquery"], default="duckdb")
    args = parser.parse_args()

    source_dir = Path(kagglehub.dataset_download(KAGGLE_DATASET))
    pipeline = dlt.pipeline(
        # Separate state per destination
        pipeline_name=f"olist_{args.destination}",
        destination=make_destination(args.destination),
        dataset_name="raw",
    )
    load_info = pipeline.run(olist_source(source_dir))
    print(load_info)


if __name__ == "__main__":
    main()

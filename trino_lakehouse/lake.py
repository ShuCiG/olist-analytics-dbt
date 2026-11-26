"""Write Olist tables as Delta with Spark and register them in Trino (#363).

    OLIST_CSV_DIR=<folder> bash tsql_parity/docker_run.sh   (Spark needs Linux or winutils: see docs/RUNNING.md)
    uv run python -m trino_lakehouse.lake --csv-dir <folder with the Olist CSVs>

Spark writes `warehouse/<table>` (Delta); Trino sees the same folder as `file:///lake/<table>` and gets the table
through `register_table`, which reads the Delta log: the metastore holds only the name and the location.
"""

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from trino_lakehouse import client
from tsql_parity import spark_port

HERE = Path(__file__).parent
WAREHOUSE = HERE / "warehouse"
SCHEMA = "olist"
CSV = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
}


def typed(table: str, df: DataFrame) -> DataFrame:
    """Text everywhere, as in raw; the two numbers the metric needs are typed."""
    if table == "order_payments":
        return df.withColumn("payment_value", F.col("payment_value").cast("decimal(18,2)")).withColumn(
            "payment_installments", F.col("payment_installments").cast("int")
        )
    return df


def write_delta(spark: SparkSession, csv_dir: Path) -> None:
    for table, file_name in CSV.items():
        df = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(str(csv_dir / file_name))
        )
        typed(table, df).write.format("delta").mode("overwrite").save(str(WAREHOUSE / table))


def register() -> None:
    """Idempotent: (re)create the schema and register every table as the admin user."""
    client.execute(f"create schema if not exists delta.{SCHEMA} with (location = 'file:///lake/{SCHEMA}')", "admin")
    for table in CSV:
        client.execute(f"drop table if exists delta.{SCHEMA}.{table}", "admin")
        client.execute(
            f"call delta.system.register_table(schema_name => '{SCHEMA}', table_name => '{table}', "
            f"table_location => 'file:///lake/{table}')",
            "admin",
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv-dir", type=Path, required=True)
    parser.add_argument("--skip-spark", action="store_true", help="only register tables already in the warehouse")
    args = parser.parse_args()
    if not args.skip_spark:
        spark = spark_port.session(HERE / "spark-warehouse")
        write_delta(spark, args.csv_dir)
        spark.stop()
    register()


if __name__ == "__main__":
    main()

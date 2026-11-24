"""PySpark + Delta port of the T-SQL procedures.

`faithful` ports the T-SQL semantics on purpose; `naive` is the port a reader would write
by translating the syntax only (half-even rounding, anti-join instead of NOT IN, GETDATE()
as the Spark session date). The runner compares the faithful port; tests compare both.
"""

import os
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from delta import configure_spark_with_delta_pip  # pyright: ignore[reportMissingTypeStubs]
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

Row = dict[str, Any]

CSV = {
    "raw_orders": "olist_orders_dataset.csv",
    "raw_order_payments": "olist_order_payments_dataset.csv",
    "raw_order_reviews": "olist_order_reviews_dataset.csv",
}


def session(warehouse: Path, ansi: bool = True) -> SparkSession:
    builder = (
        SparkSession.builder.appName("tsql-parity")  # pyright: ignore[reportUnknownMemberType]
        .master("local[2]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.ansi.enabled", str(ansi).lower())
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()  # pyright: ignore[reportUnknownVariableType]


def sql(spark: SparkSession, query: str) -> DataFrame:
    """`SparkSession.sql` with a typed signature (pyspark's overload is partly unknown)."""
    return spark.sql(query)  # pyright: ignore[reportUnknownMemberType]


def load_raw(spark: SparkSession, csv_dir: Path) -> None:
    """Same raw files as the T-SQL side, all columns as text, stored as Delta tables."""
    for table, file_name in CSV.items():
        (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(str(csv_dir / file_name))
            .write.format("delta")
            .mode("overwrite")
            .saveAsTable(table)
        )


def payment_installments(spark: SparkSession, naive: bool = False) -> DataFrame:
    p = spark.table("raw_order_payments").select(
        "order_id",
        F.col("payment_sequential").cast("int").alias("payment_sequential"),
        F.col("payment_value").cast("decimal(18,2)").alias("payment_value"),
        F.col("payment_installments").cast("int").alias("inst"),
    )
    divisor = F.when(F.col("inst") == 0, 1).otherwise(F.col("inst"))
    quotient = F.col("payment_value") / divisor
    # T-SQL ROUND rounds half away from zero = Spark round (HALF_UP); bround is half-even
    rounded = F.bround(quotient, 2) if naive else F.round(quotient, 2)
    w = Window.partitionBy("order_id").orderBy("payment_sequential")
    return p.select(
        "order_id",
        "payment_sequential",
        "payment_value",
        rounded.cast("decimal(18,2)").alias("installment_value"),
        F.sum("payment_value").over(w).cast("decimal(18,2)").alias("running_total"),
    )


def delivery_metrics(spark: SparkSession, as_of: date, naive: bool = False) -> DataFrame:
    o = spark.table("raw_orders").select(
        "order_id",
        "order_status",
        F.col("order_purchase_timestamp").cast("timestamp").alias("purchased_at"),
        F.col("order_delivered_customer_date").cast("timestamp").alias("delivered_at"),
        F.col("order_estimated_delivery_date").cast("timestamp").alias("estimated_at"),
    )
    # naive: current_date() is GETDATE() carried over; faithful: the pinned parameter
    today = F.current_date() if naive else F.lit(as_of)
    est = F.col("estimated_at").cast("date")
    return o.select(
        "order_id",
        F.datediff(F.col("delivered_at").cast("date"), F.col("purchased_at").cast("date")).alias("days_to_deliver"),
        F.when(F.col("delivered_at").isNull(), 0)
        .when(F.col("delivered_at").cast("date") > est, 1)
        .otherwise(0)
        .alias("is_late"),
        F.when(
            F.col("delivered_at").isNull() & ~F.col("order_status").isin("canceled", "unavailable") & (est < today),
            F.datediff(today, est),
        ).alias("days_overdue_open"),
    )


def orders_without_reviews(spark: SparkSession, naive: bool = False) -> DataFrame:
    orders, reviews = spark.table("raw_orders"), spark.table("raw_order_reviews")
    if naive:  # anti-join: ignores NULLs in the right side
        return orders.join(reviews, "order_id", "left_anti").select("order_id")
    reviews.createOrReplaceTempView("_reviews")
    orders.createOrReplaceTempView("_orders")
    return sql(spark, "select order_id from _orders where order_id not in (select order_id from _reviews)")


def payment_type_summary(spark: SparkSession) -> DataFrame:
    p = spark.table("raw_order_payments")
    by_type = p.groupBy("payment_type").agg(
        F.count("*").alias("payments"),
        F.sum(F.col("payment_value").cast("decimal(18,2)")).alias("total_value"),
        F.sum(F.col("payment_installments").cast("int")).alias("total_installments"),
    )
    ratio = F.col("total_value") / F.when(F.col("total_installments") == 0, None).otherwise(F.col("total_installments"))
    return by_type.select(
        "payment_type",
        "payments",
        "total_value",
        F.col("total_installments").cast("long").alias("total_installments"),
        ratio.cast("decimal(18,4)").alias("value_per_installment"),
    )


def collect(df: DataFrame) -> list[Row]:
    return [{k: (Decimal(str(v)) if isinstance(v, float) else v) for k, v in r.asDict().items()} for r in df.collect()]


def default_warehouse() -> Path:
    return Path(os.environ.get("SPARK_WAREHOUSE", Path(__file__).parent / ".spark-warehouse"))

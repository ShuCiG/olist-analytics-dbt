"""RFM segmentation of customers: the project's one dbt Python model (D81).

Grain: one row per person (customer_unique_id), as in dim_customers.
Built on Databricks (a serverless job, not the SQL warehouse) and DuckDB (in the
dbt process); disabled on BigQuery, where Python models need Dataproc. Both
engines hand dbt.ref() back as their own dataframe type, so the scoring runs in
pandas and works the same everywhere.
"""

import math

import pandas as pd


def to_pandas(relation) -> pd.DataFrame:
    # Databricks: Spark DataFrame; DuckDB: DuckDBPyRelation
    if hasattr(relation, "toPandas"):
        return relation.toPandas()
    return relation.df()


def quintile_score(values: pd.Series) -> pd.Series:
    """1-5 by percentile rank; ties share the higher score, so equal values never split."""
    pct = values.rank(method="max", pct=True)
    return pct.map(lambda p: max(1, math.ceil(p * 5))).astype("int64")


def segment(row) -> str:
    r, f, m = row.recency_score, row.frequency_score, row.monetary_score
    if f >= 2:
        return "champions" if r >= 4 else "loyal_at_risk"
    if r >= 4:
        return "new_big_spenders" if m >= 4 else "new_customers"
    if r >= 2:
        return "big_spenders_at_risk" if m >= 4 else "hibernating"
    return "lost"


def model(dbt, session):
    dbt.config(materialized="table")

    customers = to_pandas(
        dbt.ref("dim_customers").select(
            "customer_unique_id", "last_order_at", "orders_count", "lifetime_value"
        )
    )

    # "Today" is the last order in the data, not the wall clock: the data is a
    # 2016-2018 dump (same idea as Elementary's clock in dbt_project.yml)
    last_order_at = pd.to_datetime(customers["last_order_at"])
    as_of = last_order_at.max()

    rfm = pd.DataFrame(
        {
            "customer_unique_id": customers["customer_unique_id"].astype(str),
            "recency_days": (as_of - last_order_at).dt.days.astype("int64"),
            "orders_count": customers["orders_count"].astype("int64"),
            # decimal on both engines; float for ranking and the output
            "lifetime_value": customers["lifetime_value"].astype(float).round(2),
        }
    )
    # Fewer days since the last order = better, so rank the negated value
    rfm["recency_score"] = quintile_score(-rfm["recency_days"])
    # ~97% of people order once, so quintiles of orders_count would put nearly
    # everyone in one bucket: the score is the order count, capped at 5
    rfm["frequency_score"] = rfm["orders_count"].clip(upper=5).astype("int64")
    rfm["monetary_score"] = quintile_score(rfm["lifetime_value"])
    rfm["rfm_score"] = (
        rfm["recency_score"].astype(str)
        + rfm["frequency_score"].astype(str)
        + rfm["monetary_score"].astype(str)
    )
    rfm["segment"] = rfm.apply(segment, axis=1).astype(str)
    rfm["as_of_date"] = as_of.date()

    return rfm

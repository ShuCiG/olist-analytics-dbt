"""RFM segmentation of customers: the project's one dbt Python model (D81).

Grain: one row per person (customer_unique_id), as in dim_customers.
Built on Databricks (a serverless job, not the SQL warehouse) and DuckDB (in the
dbt process); disabled on BigQuery, where Python models need Dataproc. Both
engines hand dbt.ref() back as their own dataframe type, so the scoring runs in
pandas and works the same everywhere.
"""

import numpy as np
import pandas as pd


def to_pandas(relation) -> pd.DataFrame:
    # Databricks: Spark DataFrame; DuckDB: DuckDBPyRelation
    if hasattr(relation, "toPandas"):
        return relation.toPandas()
    return relation.df()


def quintile_score(values: pd.Series) -> pd.Series:
    """1-5 by percentile rank; ties share the higher score, so equal values never split."""
    pct = values.rank(method="max", pct=True)
    # vectorized: a per-row Python map ran out of memory at 9.6M customers (#317)
    return np.ceil(pct * 5).clip(lower=1).astype("int64")


def segment(rfm: pd.DataFrame) -> pd.Series:
    r, f, m = rfm["recency_score"], rfm["frequency_score"], rfm["monetary_score"]
    conditions = [
        (f >= 2) & (r >= 4),
        f >= 2,
        (r >= 4) & (m >= 4),
        r >= 4,
        (r >= 2) & (m >= 4),
        r >= 2,
    ]
    choices = [
        "champions",
        "loyal_at_risk",
        "new_big_spenders",
        "new_customers",
        "big_spenders_at_risk",
        "hibernating",
    ]
    return pd.Series(np.select(conditions, choices, default="lost"), index=rfm.index)


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
    rfm["segment"] = segment(rfm)
    rfm["as_of_date"] = as_of.date()

    return rfm

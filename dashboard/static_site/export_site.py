"""Build the static dashboard (GitHub Pages, /dashboard/, D102): page + Parquet aggregates.

Reads the DuckDB marts only, never BigQuery. KPIs come from dashboard/kpis.py, the
same SQL as the Streamlit tiles, so dashboard/tests/test_metrics.py can hold them to
the semantic layer. Published data is aggregates only: no column identifies a person
or an order, and no row covers fewer than MIN_GROUP orders or people.

    uv run python dashboard/static_site/export_site.py <output-dir>
"""

import datetime as dt
import shutil
import sys
from pathlib import Path

import duckdb
import pandas as pd

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parent))  # Streamlit-style imports

import kpis  # noqa: E402
import warehouse  # noqa: E402
from warehouse import DEFAULT_PERIOD, LOST_STATUSES, date_filter, table  # noqa: E402

# Customers and rates don't add up across months, so a free date range can't be
# computed from aggregates: the page offers these periods, each precomputed.
PERIODS = {
    "all": ("Jan 2017 – Aug 2018", DEFAULT_PERIOD),
    "2017": ("2017", (dt.date(2017, 1, 1), dt.date(2017, 12, 31))),
    "2018": ("Jan – Aug 2018", (dt.date(2018, 1, 1), dt.date(2018, 8, 31))),
}
MIN_GROUP = 10
MAX_RETENTION_MONTH = 12
# Column in each table that holds its group size
GROUP_SIZE = {
    "monthly": "orders",
    "categories": "orders",
    "payments": "orders",
    "states": "orders",
    "review_scores": "orders",
    "retention": "customers",
}
PAGE_FILES = ("index.html", "page.js", "page.css", "queries.json")
DATA_NOTICE = """\
Aggregates derived from the Brazilian E-Commerce Public Dataset by Olist
https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce

Licensed under CC BY-NC-SA 4.0: https://creativecommons.org/licenses/by-nc-sa/4.0/
Changes: orders aggregated by period, month, category, payment method, state and review score.
These files are shared under the same licence, for non-commercial use.
"""

_LOST = ", ".join(f"'{s}'" for s in LOST_STATUSES)


def kpi_row(period_id: str) -> dict[str, object]:
    label, period = PERIODS[period_id]
    sales, delivery, cohorts = kpis.sales(period), kpis.delivery(period), kpis.cohorts(period)
    return {
        "period_id": period_id,
        "period_label": label,
        "orders": sales.orders,
        "customers": sales.customers,
        "revenue": sales.revenue,
        "avg_order_value": sales.avg_order_value,
        "delivered_orders": delivery.orders,
        "avg_days_to_deliver": delivery.avg_days,
        "late_delivery_rate": delivery.late_rate,
        "avg_review_score": delivery.avg_score,
        "new_customers": cohorts.new_customers,
        "retention_m1": cohorts.retention_m1,
        "retention_m3": cohorts.retention_m3,
        "ltv_3m": cohorts.ltv_3m,
    }


def monthly(period: kpis.Period) -> pd.DataFrame:
    return warehouse.query(f"""
        select
            cast(extract(year from purchase_date) as integer) as yr,
            cast(extract(month from purchase_date) as integer) as mo,
            count(*) as orders,
            sum(case when order_status not in ({_LOST}) then order_amount end) as revenue,
            avg(case when is_delivered then days_to_deliver end) as avg_days_to_deliver,
            avg(case when is_delivered then case when is_delivered_late then 1.0 else 0.0 end end)
                as late_delivery_rate
        from {table('fct_orders')}
        where {date_filter(period)}
        group by 1, 2
        order by 1, 2
    """)


def categories(period: kpis.Period) -> pd.DataFrame:
    return warehouse.query(f"""
        select
            coalesce(p.category_name, 'unknown') as category,
            sum(i.item_amount) as revenue,
            count(distinct i.order_id) as orders
        from {table('fct_order_items')} as i
        left join {table('dim_products')} as p on i.product_id = p.product_id
        where {date_filter(period, 'i.purchase_date')} and i.order_status not in ({_LOST})
        group by 1
        order by revenue desc
        limit 10
    """)


def payments(period: kpis.Period) -> pd.DataFrame:
    where = date_filter(period)
    amounts = warehouse.query(f"""
        select 'Credit card' as method, sum(credit_card_amount) as amount,
            sum(case when credit_card_amount > 0 then 1 else 0 end) as orders
        from {table('fct_orders')} where {where}
        union all
        select 'Boleto', sum(boleto_amount), sum(case when boleto_amount > 0 then 1 else 0 end)
        from {table('fct_orders')} where {where}
        union all
        select 'Voucher', sum(voucher_amount), sum(case when voucher_amount > 0 then 1 else 0 end)
        from {table('fct_orders')} where {where}
        union all
        select 'Debit card', sum(debit_card_amount), sum(case when debit_card_amount > 0 then 1 else 0 end)
        from {table('fct_orders')} where {where}
    """)
    amounts["share"] = amounts["amount"] / amounts["amount"].sum()
    return amounts.sort_values("share", ascending=False)


def states(period: kpis.Period) -> pd.DataFrame:
    return warehouse.query(f"""
        select
            c.state,
            count(*) as orders,
            avg(case when o.is_delivered_late then 1.0 else 0.0 end) as late_delivery_rate
        from {table('fct_orders')} as o
        inner join {table('dim_customers')} as c on o.customer_key = c.customer_key
        where o.is_delivered and {date_filter(period, 'o.purchase_date')}
        group by 1
        having count(*) >= 100
        order by late_delivery_rate desc
        limit 10
    """)


def review_scores(period: kpis.Period) -> pd.DataFrame:
    scores = warehouse.query(f"""
        select
            case when is_delivered_late then 'Late' else 'On time' end as delivery,
            cast(review_score as integer) as review_score,
            count(*) as orders
        from {table('fct_orders')}
        where is_delivered and has_review and {date_filter(period)}
        group by 1, 2
        order by 1, 2
    """)
    scores["share"] = scores["orders"] / scores.groupby("delivery")["orders"].transform("sum")
    return scores


def retention(period: kpis.Period) -> pd.DataFrame:
    # Later months of young cohorts hold a handful of people: left out by MIN_GROUP
    return warehouse.query(f"""
        select
            cast(months_since_first_order as integer) as months_since_first_order,
            sum(active_customers) as customers,
            sum(active_customers) / sum(cohort_size) as retention_rate,
            sum(cumulative_revenue) / sum(cohort_size) as value_per_customer
        from {table('fct_customer_cohorts')}
        where {kpis.cohort_filter(period)} and months_since_first_order <= {MAX_RETENTION_MONTH}
        group by 1
        having sum(active_customers) >= {MIN_GROUP}
        order by 1
    """)


def tables() -> dict[str, pd.DataFrame]:
    if warehouse.BACKEND != "duckdb":
        raise SystemExit("The static dashboard is built from DuckDB only (DASHBOARD_BACKEND=duckdb)")
    per_period = {
        "monthly": monthly,
        "categories": categories,
        "payments": payments,
        "states": states,
        "review_scores": review_scores,
        "retention": retention,
    }
    built = {"kpis": pd.DataFrame([kpi_row(period_id) for period_id in PERIODS])}
    for name, query in per_period.items():
        frames = [query(period).assign(period_id=pid) for pid, (_, period) in PERIODS.items()]
        built[name] = pd.concat(frames, ignore_index=True)
        smallest = built[name][GROUP_SIZE[name]].min()
        if smallest < MIN_GROUP:
            raise SystemExit(f"{name}: a row covers {smallest} (< {MIN_GROUP}); it would not be an aggregate")
    return built


def write_parquet(frames: dict[str, pd.DataFrame], out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    for name, frame in frames.items():
        con.register("frame", frame)
        con.execute(f"copy frame to '{(out / f'{name}.parquet').as_posix()}' (format parquet, compression zstd)")
        con.unregister("frame")


def build(out: Path) -> None:
    write_parquet(tables(), out / "data")
    (out / "data" / "NOTICE.txt").write_text(DATA_NOTICE, encoding="utf-8", newline="\n")
    for name in PAGE_FILES:
        shutil.copy(HERE / name, out / name)


if __name__ == "__main__":
    build(Path(sys.argv[1]))

import altair as alt
import pandas as pd
import streamlit as st

import charts
from warehouse import ml_table, query

# BigQuery SQL: the BigQuery ML marts exist only there (the page is registered on that backend only)
BACKTEST = "ml_sales_forecast_backtest"
ALL = "all categories"

st.title("Forecasts & repeat buyers (BigQuery ML)")
st.caption(
    "Two models trained and applied in SQL by dbt on BigQuery ML (prod `marts`). "
    "Sales: ARIMA_PLUS per category on weekly orders and revenue, checked on the last "
    "13 weeks of history it didn't see (backtest), then forecast 13 weeks ahead. "
    "Repeat buyers: logistic regression on a person's first order, will they order again "
    "within 180 days. The purchase dates in the sidebar don't apply here."
)

metrics = query(f"""
    select model_name, category_name, measure, feature_name, metric_name, metric_value
    from {ml_table('fct_ml_model_metrics')}
""")
if metrics.empty:
    st.info("The BigQuery ML marts are empty: the next prod deploy that builds them fills this page.")
    st.stop()


def metric_rows(model: str, names: list[str]) -> pd.DataFrame:
    return metrics.loc[(metrics["model_name"] == model) & metrics["metric_name"].isin(names)]


def metric(model: str, name: str, category: str | None = None, measure: str | None = None) -> float:
    rows = metric_rows(model, [name])
    if category is not None:
        rows = rows.loc[(rows["category_name"] == category) & (rows["measure"] == measure)]
    return float(rows["metric_value"].iloc[0]) if len(rows) else float("nan")


next_quarter = query(f"""
    select sum(predicted_value) as revenue
    from {ml_table('fct_sales_forecast')}
    where prediction_type = 'forecast' and category_name = '{ALL}' and measure = 'revenue'
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Revenue forecast, next 13 weeks", f"R$ {next_quarter['revenue'] / 1e6:,.1f} M", border=True)
c2.metric(
    "Backtest error, 13-week revenue", f"{metric(BACKTEST, 'holdout_total_ape', ALL, 'revenue'):.1%}",
    border=True, help="The 13 held-out weeks summed up, predicted vs actual, all categories.",
)
c3.metric(
    "Repeat model ROC AUC", f"{metric('ml_repeat_purchase', 'roc_auc'):.2f}",
    border=True, help="On the evaluation sample: first orders of the last 90 labelled days. 0.5 = guessing.",
)
c4.metric(
    "Top-tenth lift", f"{metric('ml_repeat_purchase', 'top_decile_lift'):.1f}×",
    border=True, help="Repeat rate of the tenth with the highest scores / the evaluation sample's rate.",
)

st.subheader("Sales forecast vs actuals")
series = query(f"""
    select distinct category_name
    from {ml_table('fct_sales_forecast')}
    order by category_name
""")["category_name"].tolist()
left, right = st.columns([3, 1])
category = left.selectbox("Series", series, index=series.index(ALL) if ALL in series else 0) or ALL
measure = right.radio("Measure", ["revenue", "orders"], horizontal=True)

weeks = query(f"""
    select week_start_date, actual_value, predicted_value, predicted_lower_bound, predicted_upper_bound
    from {ml_table('fct_sales_forecast')}
    where category_name = '{category.replace("'", "''")}' and measure = '{measure}'
    order by week_start_date
""")
charts.forecast(weeks, "Revenue, R$" if measure == "revenue" else "Orders", ",.0f")
st.caption(
    f"Backtest on the 13 held-out weeks: weekly MAPE "
    f"{metric(BACKTEST, 'mean_absolute_percentage_error', category, measure) / 100:.1%}, "
    f"13-week total off by {metric(BACKTEST, 'holdout_total_ape', category, measure):.1%}. "
    "The band is the 90 % prediction interval."
)

with st.expander("Backtest error per series"):
    # ML.EVALUATE reports MAPE in percent, the mart's own ratio as a fraction
    accuracy = (
        metric_rows(BACKTEST, ["holdout_total_ape", "mean_absolute_percentage_error"])
        .assign(metric_value=lambda d: d["metric_value"].where(
            d["metric_name"] == "holdout_total_ape", d["metric_value"] / 100
        ))
        .pivot_table(index="category_name", columns=["measure", "metric_name"], values="metric_value")
    )
    accuracy.columns = [
        f"{m}: {'13-week total' if n == 'holdout_total_ape' else 'weekly MAPE'}" for m, n in accuracy.columns
    ]
    st.dataframe(
        accuracy.reset_index(),
        hide_index=True,
        width="stretch",
        column_config={
            "category_name": st.column_config.TextColumn("Series"),
            **{c: st.column_config.NumberColumn(c, format="percent") for c in accuracy.columns},
        },
    )

st.subheader("Who comes back")
deciles = query(f"""
    select
        probability_decile,
        avg(if(ordered_again, 1, 0)) as repeat_rate,
        count(*) as people
    from {ml_table('fct_repeat_purchase_scores')}
    where sample = 'eval'
    group by probability_decile
    order by probability_decile
""").assign(probability_decile=lambda d: d.probability_decile.astype(str))
features = metric_rows("ml_repeat_purchase", ["attribution"]).sort_values("metric_value", ascending=False).head(10)
left, right = st.columns(2)
with left:
    st.markdown("**Repeat rate by score tenth** (evaluation sample, 1 = highest scores)")
    charts.bar_v(
        deciles, "probability_decile", "repeat_rate", "Ordered again within 180 days", ".1%",
        order=[str(i) for i in range(1, 11)],
        extra_tooltip=[alt.Tooltip("people:Q", title="People", format=",")],
    )
    st.caption(f"All of the evaluation sample: {metric('ml_repeat_purchase', 'eval_repeat_rate'):.1%}.")
with right:
    st.markdown("**What the score depends on most** (global attribution)")
    charts.bar_h(features, "metric_value", "feature_name", "Attribution", ".3f")

recent = query(f"""
    select count(*) as people
    from {ml_table('fct_repeat_purchase_scores')}
    where sample = 'unlabelled' and probability_decile = 1
""").iloc[0]
st.caption(
    f"{recent['people']:,.0f} recent first-time buyers (too recent for a label) score in their top tenth: "
    "the list a retention campaign would start from (`fct_repeat_purchase_scores`, by `customer_key`)."
)

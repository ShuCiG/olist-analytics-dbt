import altair as alt
import pandas as pd
import streamlit as st

import charts
from warehouse import period_filter, query, table

st.title("What customers write")
st.caption(
    "Review texts labelled with a sentiment and a topic, with an English translation "
    "(`fct_review_topics`). Labels are made by an LLM (Claude Code) following "
    "[the guidelines](https://github.com/ShuCiG/olist-analytics-dbt/blob/main/ingestion/review_labels/guidelines.md); "
    "reviews without text, and texts not labelled yet, are not counted."
)

TOPICS = {
    "delivery": "Delivery",
    "product_quality": "Product quality",
    "wrong_or_missing_item": "Wrong or missing item",
    "seller_service": "Seller service",
    "general": "General (no aspect)",
    "other": "Other (price, packaging, ...)",
}

in_period = period_filter()

topics = query(f"""
    select
        topic,
        count(*) as reviews,
        avg(case when sentiment in ('negative', 'mixed') then 1.0 else 0.0 end) as complaint_rate,
        avg(case when is_delivered_late then 1.0 else 0.0 end) as late_rate,
        avg(case when is_delivered then 0.0 else 1.0 end) as undelivered_rate,
        avg(review_score) as avg_score
    from {table('fct_review_topics')}
    where {in_period}
    group by 1
""")

if topics.empty:
    st.info("No labelled reviews in the selected period.")
    st.stop()

kpi = query(f"""
    select
        count(*) as reviews,
        avg(case when sentiment in ('negative', 'mixed') then 1.0 else 0.0 end) as complaint_rate,
        sum(case when sentiment in ('negative', 'mixed') and topic = 'delivery' then 1.0 else 0.0 end)
            / nullif(sum(case when sentiment in ('negative', 'mixed') then 1.0 else 0.0 end), 0)
            as delivery_share_of_complaints,
        avg(case when topic = 'delivery' then case when is_delivered_late then 1.0 else 0.0 end end)
            as late_rate_delivery_topic,
        avg(case when topic <> 'delivery' then case when is_delivered_late then 1.0 else 0.0 end end)
            as late_rate_other_topics
    from {table('fct_review_topics')}
    where {in_period}
""").iloc[0]


def pct(value: float) -> str:
    return "–" if pd.isna(value) else f"{value:.0%}"


c1, c2, c3, c4 = st.columns(4)
c1.metric("Labelled reviews", f"{int(kpi['reviews']):,}", border=True)
c2.metric("Complaints (negative or mixed)", pct(kpi["complaint_rate"]), border=True)
c3.metric("Complaints about delivery", pct(kpi["delivery_share_of_complaints"]), border=True)
c4.metric(
    "Late: delivery topic vs others",
    f"{pct(kpi['late_rate_delivery_topic'])} vs {pct(kpi['late_rate_other_topics'])}",
    help="Share of orders delivered after the estimated date, for reviews about delivery vs all other topics.",
    border=True,
)

topics["topic_label"] = [TOPICS[t] for t in topics["topic"]]
topics["share"] = topics["reviews"] / int(kpi["reviews"])
left, right = st.columns(2)
with left:
    st.subheader("What reviews are about")
    charts.bar_h(
        topics, "share", "topic_label", "Share of labelled reviews", ".0%",
        extra_tooltip=[
            alt.Tooltip("reviews:Q", title="Reviews", format=","),
            alt.Tooltip("complaint_rate:Q", title="Negative or mixed", format=".0%"),
            alt.Tooltip("avg_score:Q", title="Avg score", format=".2f"),
        ],
    )
with right:
    st.subheader("Late deliveries by review topic")
    charts.bar_h(
        topics, "late_rate", "topic_label", "Order delivered late", ".0%",
        extra_tooltip=[
            alt.Tooltip("reviews:Q", title="Reviews", format=","),
            alt.Tooltip("undelivered_rate:Q", title="Not delivered", format=".0%"),
        ],
    )
    st.caption("The labels come from the text alone; lateness comes from the order's delivery dates.")

MIN_REVIEWS = 30
categories = query(f"""
    select
        category_name,
        count(*) as reviews,
        avg(case when sentiment in ('negative', 'mixed') then 1.0 else 0.0 end) as complaint_rate,
        avg(case when sentiment in ('negative', 'mixed') and topic = 'product_quality' then 1.0 else 0.0 end)
            as quality_complaint_rate
    from {table('fct_review_topics')}
    where {in_period} and category_name is not null
    group by 1
    having count(*) >= {MIN_REVIEWS}
    order by complaint_rate desc
    limit 15
""")

st.subheader("Complaint rate by category")
if categories.empty:
    st.caption(f"No category has {MIN_REVIEWS} labelled reviews in the period yet.")
else:
    charts.bar_h(
        categories, "complaint_rate", "category_name", "Negative or mixed reviews", ".0%",
        extra_tooltip=[
            alt.Tooltip("reviews:Q", title="Labelled reviews", format=","),
            alt.Tooltip("quality_complaint_rate:Q", title="Complaints about product quality", format=".0%"),
        ],
    )
    st.caption(f"Top 15 categories with at least {MIN_REVIEWS} labelled reviews in the period.")

st.subheader("Latest complaints, translated")
topic_pick = st.selectbox("Topic", list(TOPICS), format_func=lambda t: TOPICS[t], index=0)
latest = query(f"""
    select
        translation_en,
        review_score,
        category_name,
        customer_state,
        purchase_date
    from {table('fct_review_topics')}
    where {in_period}
        and sentiment in ('negative', 'mixed')
        and topic = '{topic_pick}'
    order by purchase_date desc, order_id
    limit 10
""")
st.dataframe(
    latest,
    hide_index=True,
    width="stretch",
    column_config={
        "translation_en": st.column_config.TextColumn("Review (English)", width="large"),
        "review_score": st.column_config.NumberColumn("Score", format="%d ★"),
        "category_name": "Category",
        "customer_state": "State",
        "purchase_date": st.column_config.DateColumn("Purchased"),
    },
)
st.caption("The ten latest negative or mixed reviews on the topic in the period.")

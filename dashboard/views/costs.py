import altair as alt
import streamlit as st

import charts
from warehouse import costs_table, query

# BigQuery SQL: the cost mart exists only there (the page is registered on that backend only)
DAYS = 30
MIB = 1024**2
GIB = 1024**3

st.title("dbt cost on BigQuery")
st.caption(
    f"BigQuery jobs that dbt ran in the last {DAYS} days, by the labels dbt puts on "
    "each job (`fct_dbt_job_costs`, refreshed by every prod deploy). Bytes billed at "
    "on-demand rates: at least 10 MiB per table a query reads, 0 on a cache hit. "
    "The purchase dates in the sidebar don't apply here."
)

recent = f"created_date >= date_sub(current_date(), interval {DAYS} day)"
mart = costs_table()

kpi = query(f"""
    select
        count(*) as jobs,
        coalesce(sum(bytes_billed), 0) as billed,
        count(distinct dbt_invocation_id) as commands,
        coalesce(avg(case when is_cache_hit then 1.0 else 0.0 end), 0) as cache_rate
    from {mart}
    where {recent}
""").iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Bytes billed", f"{kpi['billed'] / GIB:,.2f} GiB", border=True)
c2.metric("dbt jobs", f"{kpi['jobs']:,.0f}", border=True)
c3.metric("dbt commands", f"{kpi['commands']:,.0f}", border=True, help="dbt invocations: build, freshness, docs, ...")
c4.metric("Cache hits", f"{kpi['cache_rate']:.0%}", border=True)

if not kpi["jobs"]:
    st.info(f"No dbt jobs in the last {DAYS} days yet: the next prod deploy fills the table.")
    st.stop()

targets = query(f"""
    select dbt_target, count(*) as jobs, sum(bytes_billed) / {MIB} as mib
    from {mart}
    where {recent}
    group by 1
""")

nodes = query(f"""
    select
        concat(dbt_resource_type, ': ', coalesce(dbt_node, '(run-level queries)')) as node,
        count(*) as jobs,
        sum(bytes_billed) / {MIB} as mib,
        sum(slot_ms) / 1000 as slot_s
    from {mart}
    where {recent} and dbt_target = 'prod'
    group by 1
    order by mib desc
    limit 15
""")

left, right = st.columns(2)
with left:
    st.subheader("Bytes billed per dbt target")
    charts.bar_h(
        targets, "mib", "dbt_target", "MiB billed", ",.0f",
        extra_tooltip=[alt.Tooltip("jobs:Q", title="Jobs", format=",")],
    )
    st.caption("prod = deploys from GitHub Actions, airflow = the hourly replay, ci = pull requests.")
with right:
    st.subheader("Most expensive nodes in prod")
    charts.bar_h(
        nodes, "mib", "node", "MiB billed", ",.0f",
        extra_tooltip=[
            alt.Tooltip("jobs:Q", title="Jobs", format=","),
            alt.Tooltip("slot_s:Q", title="Slot seconds", format=",.1f"),
        ],
    )

st.subheader("Latest prod dbt commands")
commands = query(f"""
    select
        min(created_at) as started_at,
        count(*) as jobs,
        sum(bytes_billed) / {MIB} as mib,
        sum(slot_ms) / 1000 as slot_s,
        countif(is_failed) as failed_jobs,
        dbt_invocation_id
    from {mart}
    where {recent} and dbt_target = 'prod'
    group by dbt_invocation_id
    order by started_at desc
    limit 20
""")
st.dataframe(
    commands,
    hide_index=True,
    column_config={
        "started_at": st.column_config.DatetimeColumn("Started (UTC)", format="YYYY-MM-DD HH:mm"),
        "jobs": st.column_config.NumberColumn("Jobs", format="%d"),
        "mib": st.column_config.NumberColumn("MiB billed", format="%.0f"),
        "slot_s": st.column_config.NumberColumn("Slot seconds", format="%.1f"),
        "failed_jobs": st.column_config.NumberColumn("Failed jobs", format="%d"),
        "dbt_invocation_id": st.column_config.TextColumn("dbt invocation"),
    },
)

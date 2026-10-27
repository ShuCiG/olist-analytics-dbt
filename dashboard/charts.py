"""Altair chart helpers: thin marks, one hue per series, tooltips on every mark.

Colors are the first two slots of a CVD-validated categorical palette, with
separate steps for the light and dark theme (not an automatic flip).
"""

import altair as alt
import pandas as pd
import streamlit as st

_SERIES = {
    "light": ["#2a78d6", "#eb6834"],
    "dark": ["#3987e5", "#d95926"],
}


def series_colors() -> list[str]:
    return _SERIES["dark" if st.context.theme.type == "dark" else "light"]


def _show(chart: alt.Chart | alt.LayerChart | alt.FacetChart, height: int) -> None:
    st.altair_chart(chart.properties(height=height), width="stretch")


def line(df: pd.DataFrame, y: str, title: str, fmt: str, height: int = 260) -> None:
    """Single-series monthly line (`month` column on x) with hover points."""
    base = alt.Chart(df).encode(
        x=alt.X("month:T", title=None, axis=alt.Axis(format="%b %Y", grid=False)),
        y=alt.Y(f"{y}:Q", title=title, axis=alt.Axis(format=fmt)),
    )
    hover = alt.selection_point(fields=["month"], nearest=True, on="pointerover", empty=False)
    color = series_colors()[0]
    chart = alt.layer(
        base.mark_line(color=color, strokeWidth=2),
        base.mark_point(color=color, filled=True, size=64)
        .encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip("month:T", title="Month", format="%B %Y"),
                alt.Tooltip(f"{y}:Q", title=title, format=fmt),
            ],
        )
        .add_params(hover),
    )
    _show(chart, height)


def bar_h(
    df: pd.DataFrame,
    x: str,
    y: str,
    title: str,
    fmt: str,
    extra_tooltip: list[alt.Tooltip] | None = None,
    height: int | None = None,
    label_limit: int = 180,
) -> None:
    """Single-series horizontal bars, sorted by value, largest on top.

    `label_limit`: y-axis labels longer than this many pixels are cut (0: never).
    """
    chart = (
        alt.Chart(df)
        .mark_bar(color=series_colors()[0], cornerRadiusEnd=4, height={"band": 0.7})
        .encode(
            x=alt.X(f"{x}:Q", title=title, axis=alt.Axis(format=fmt)),
            y=alt.Y(f"{y}:N", title=None, sort="-x", axis=alt.Axis(labelOverlap=False, labelLimit=label_limit)),
            tooltip=[
                alt.Tooltip(f"{y}:N", title=y.replace("_", " ").capitalize()),
                alt.Tooltip(f"{x}:Q", title=title, format=fmt),
                *(extra_tooltip or []),
            ],
        )
    )
    _show(chart, height or max(160, 30 * len(df)))


def bar_grouped(
    df: pd.DataFrame, x: str, y: str, group: str, title: str, fmt: str, height: int = 260
) -> None:
    """Vertical bars for two groups side by side (legend + fixed colors per group)."""
    groups = list(dict.fromkeys(df[group]))
    chart = (
        alt.Chart(df)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            x=alt.X(f"{x}:O", title=x.replace("_", " ").capitalize(), axis=alt.Axis(labelAngle=0)),
            xOffset=alt.XOffset(f"{group}:N", sort=groups),
            y=alt.Y(f"{y}:Q", title=title, axis=alt.Axis(format=fmt)),
            color=alt.Color(
                f"{group}:N",
                sort=groups,
                scale=alt.Scale(domain=groups, range=series_colors()[: len(groups)]),
                legend=alt.Legend(title=None, orient="top"),
            ),
            tooltip=[
                alt.Tooltip(f"{group}:N", title="Group"),
                alt.Tooltip(f"{x}:O", title=x.replace("_", " ").capitalize()),
                alt.Tooltip(f"{y}:Q", title=title, format=fmt),
            ],
        )
    )
    _show(chart, height)

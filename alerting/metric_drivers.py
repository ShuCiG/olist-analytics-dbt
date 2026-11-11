"""Why a metric moved: the change between two periods split by the members of a dimension (D110).

Pure logic, no Airflow and no MetricFlow import at module level, so plain pytest covers it.
The numbers come through a `Runner`, which in production is MetricFlow (`metricflow_runner`):
the metric keeps its one definition from the semantic layer (D35, D99).

- Additive metric (sum, count): the change of a member is after - before. The members add up
  to the total change.
- Ratio metric r = N / D (both additive): with the share w of a member in the denominator,
      rate effect = w_after * (r_after - r_before)         the member got better or worse
      mix effect  = (w_after - w_before) * (r_before - R)  the member got bigger or smaller
  where R is the total ratio before. Over all members the two add up to the total change
  exactly. A member missing in one period takes its rate from the other, so it is a pure
  mix effect.
- Not decomposable by a dimension (the members overlap or the value is not a sum): distinct
  counts, averages, medians, derived and cumulative metrics, ratios built on them. Refused.

The total change is queried without a dimension; the top members are listed and everything
else, including people the dimension does not cover, is one "other" row, so the rows add up
to the total change.

A dimension of another grain (product category, seller: on the order lines, not the orders)
is reached through a metric that declares `meta: {same_total_as: <metric>}` (D113): order-level
`revenue` is split by category through line-level `item_revenue`. Both totals are queried and
must match in both periods, so the rows still add up to the change of the metric asked about.

CLI for the Airflow run email (the last stdout line is JSON):

    python -m alerting.metric_drivers --target dev
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import os
import sys
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from html import escape
from pathlib import Path
from typing import Any

from . import email_html

# Metrics the run email checks, and the dimensions it may explain them by
EMAIL_METRICS = ["revenue", "orders", "average_order_value", "late_delivery_rate"]
EMAIL_DIMENSIONS = [
    "customer__state",
    "order__order_status",
    "customer__is_repeat_customer",
    "product__category_name",
    "seller__state",
]
# Entities whose ids a change may be split by. Not `customer`: its id is a person (D94).
SPLIT_ENTITIES = ["seller", "product"]
SAME_TOTAL_TOLERANCE = 0.01  # BRL; money is rounded to cents per row
BASE_METRIC = "orders"  # "base >= 100 orders": the size of the earlier period
DEFAULT_THRESHOLD = 0.10  # |relative change|
DEFAULT_MIN_BASE = 100
TOP_MEMBERS = 3
EMAIL_DIMENSION_COUNT = 2
OTHER = "other"
NO_MEMBER = "(none)"
ADDITIVE_AGGS = {"sum", "count"}

# (metrics, group by, first day, last day inclusive) -> one row per group, a column per name
Runner = Callable[[list[str], list[str], datetime.date, datetime.date], list[dict[str, Any]]]
# metric -> the names it can be split by (categorical dimensions, allowed entities)
GroupBys = Callable[[str], set[str]]


class NotDecomposable(ValueError):
    """The metric cannot be split by a dimension; the message says why."""


@dataclass(frozen=True)
class Period:
    start: datetime.date
    end: datetime.date  # inclusive

    @property
    def label(self) -> str:
        if self.start.day == 1 and (self.end + datetime.timedelta(days=1)).day == 1 and self.start.month == self.end.month:
            return self.start.strftime("%Y-%m")
        return f"{self.start} to {self.end}"


@dataclass(frozen=True)
class MetricSpec:
    name: str
    kind: str  # additive | ratio
    numerator: str | None = None
    denominator: str | None = None
    same_total: tuple[str, ...] = ()  # metrics of another grain with the same total (meta same_total_as)


@dataclass
class Driver:
    member: str
    before: float | None  # None: no data in that period, or "other" of a ratio
    after: float | None
    delta: float  # for a ratio metric: mix + rate
    mix: float | None = None
    rate: float | None = None
    share: float | None = None  # delta / total delta


@dataclass
class Breakdown:
    dimension: str
    drivers: list[Driver]  # the top members by |delta|
    other: Driver | None
    members: int  # how many members the dimension had
    explained: float  # |sum of the top deltas| / |total delta|, 0 when the total did not change
    via: str | None = None  # the same-total metric the split was queried with, if not the metric itself

    @property
    def informative(self) -> float:
        """How much the top members say beyond their number: a flag with two values covers everything."""
        return self.explained - min(1.0, len(self.drivers) / self.members) if self.members else 0.0


@dataclass
class Explanation:
    metric: str
    kind: str
    before_period: str
    after_period: str
    before: float
    after: float
    delta: float
    relative: float | None
    breakdowns: list[Breakdown] = field(default_factory=list)


# --- which metrics can be decomposed ----------------------------------------------------------


def specs_from_manifest(manifest: dict[str, Any]) -> dict[str, MetricSpec | str]:
    """Per metric of semantic_manifest.json: its spec, or the reason it cannot be decomposed."""
    metrics = {m["name"]: m for m in manifest["metrics"]}

    def simple(name: str) -> MetricSpec | str:
        metric = metrics[name]
        if metric["type"] != "simple":
            return f"{metric['type']} metric"
        agg = (metric["type_params"].get("metric_aggregation_params") or {}).get("agg")
        if agg not in ADDITIVE_AGGS:
            return f"aggregation {agg!r} is not a sum"
        return MetricSpec(name, "additive")

    result: dict[str, MetricSpec | str] = {}
    for name, metric in metrics.items():
        if metric["type"] == "ratio":
            numerator = metric["type_params"]["numerator"]["name"]
            denominator = metric["type_params"]["denominator"]["name"]
            parts = {part: simple(part) for part in (numerator, denominator)}
            bad = [f"{part}: {why}" for part, why in parts.items() if isinstance(why, str)]
            result[name] = f"ratio over non-additive parts ({'; '.join(bad)})" if bad else MetricSpec(name, "ratio", numerator, denominator)
        else:
            result[name] = simple(name)

    same_total: dict[str, list[str]] = {}
    for name, metric in metrics.items():
        target = ((metric.get("config") or {}).get("meta") or {}).get("same_total_as")
        if not target:
            continue
        if target not in result:
            raise ValueError(f"{name!r}: same_total_as names an unknown metric {target!r}")
        for end in (name, target):
            spec = result[end]
            if not isinstance(spec, MetricSpec) or spec.kind != "additive":
                raise ValueError(f"{name!r}: same_total_as needs two sums or counts, {end!r} is not one")
        same_total.setdefault(target, []).append(name)
    for target, names in same_total.items():
        spec = result[target]
        assert isinstance(spec, MetricSpec)
        result[target] = MetricSpec(spec.name, spec.kind, same_total=tuple(sorted(names)))
    return result


def spec_for(specs: dict[str, MetricSpec | str], metric: str) -> MetricSpec:
    if metric not in specs:
        raise ValueError(f"Unknown metric {metric!r}. Available: {', '.join(sorted(specs))}")
    spec = specs[metric]
    if isinstance(spec, str):
        raise NotDecomposable(
            f"{metric!r} cannot be split by a dimension: {spec}. Only sums, counts and ratios of those can be, "
            "because the members of a dimension must add up to the total."
        )
    return spec


def source_metric(spec: MetricSpec, dimension: str, group_bys: GroupBys | None) -> str | None:
    """The metric to split `spec` by `dimension` with: itself, a same-total metric, or None (unreachable)."""
    if group_bys is None or dimension in group_bys(spec.name):
        return spec.name
    return next((other for other in spec.same_total if dimension in group_bys(other)), None)


def reachable(spec: MetricSpec, group_bys: GroupBys) -> set[str]:
    """Every name `spec` can be split by, directly or through a same-total metric."""
    return set().union(group_bys(spec.name), *(group_bys(other) for other in spec.same_total))


# --- maths -------------------------------------------------------------------------------------


def _number(value: Any) -> float:
    if value is None:
        return 0.0
    return float(value)  # Decimal included


def _member(value: Any) -> str:
    return NO_MEMBER if value is None else str(value).lower() if isinstance(value, bool) else str(value)


def _by_member(rows: list[dict[str, Any]], dimension: str, metrics: list[str]) -> dict[str, dict[str, float]]:
    table: dict[str, dict[str, float]] = {}
    for row in rows:
        member = _member(row[dimension])
        current = table.setdefault(member, dict.fromkeys(metrics, 0.0))
        for metric in metrics:
            current[metric] += _number(row[metric])
    return table


def _relative(before: float, after: float) -> float | None:
    return (after - before) / abs(before) if before else None


def _total(runner: Runner, metrics: list[str], period: Period) -> dict[str, float]:
    rows = runner(metrics, [], period.start, period.end)
    row = rows[0] if rows else {}
    return {metric: _number(row.get(metric)) for metric in metrics}


def additive_drivers(before: dict[str, float], after: dict[str, float], total_delta: float, top: int) -> tuple[list[Driver], Driver | None]:
    """Members ranked by |change|; the rest, and whatever the members do not cover, as one row."""
    rows = [
        Driver(member, before.get(member, 0.0), after.get(member, 0.0), after.get(member, 0.0) - before.get(member, 0.0))
        for member in before.keys() | after.keys()
    ]
    return _rank(rows, total_delta, top, sums=True)


def ratio_drivers(
    before: dict[str, tuple[float, float]],
    after: dict[str, tuple[float, float]],
    total_before: float,
    total_delta: float,
    top: int,
) -> tuple[list[Driver], Driver | None]:
    """Members of a ratio, each (numerator, denominator) per period; mix and rate effects."""
    den_before = sum(d for _, d in before.values())
    den_after = sum(d for _, d in after.values())
    rows: list[Driver] = []
    for member in before.keys() | after.keys():
        n0, d0 = before.get(member, (0.0, 0.0))
        n1, d1 = after.get(member, (0.0, 0.0))
        w0 = d0 / den_before if den_before else 0.0
        w1 = d1 / den_after if den_after else 0.0
        r0 = n0 / d0 if d0 else None
        r1 = n1 / d1 if d1 else None
        if r0 is None and r1 is None:
            continue
        r0 = r1 if r0 is None else r0  # a new member: a pure mix effect
        r1 = r0 if r1 is None else r1  # a vanished one too
        assert r0 is not None and r1 is not None
        mix = (w1 - w0) * (r0 - total_before)
        rate = w1 * (r1 - r0)
        rows.append(Driver(member, r0 if d0 else None, r1 if d1 else None, mix + rate, mix, rate))
    return _rank(rows, total_delta, top)


def _rank(rows: list[Driver], total_delta: float, top: int, sums: bool = False) -> tuple[list[Driver], Driver | None]:
    rows.sort(key=lambda d: (-abs(d.delta), d.member))
    shown, rest = rows[:top], rows[top:]
    for driver in shown:
        driver.share = driver.delta / total_delta if total_delta else None
    other_delta = total_delta - sum(d.delta for d in shown)
    if not rest and abs(other_delta) < 1e-9 * max(1.0, abs(total_delta)):
        return shown, None
    other = Driver(
        OTHER,
        sum(d.before or 0.0 for d in rest) if sums else None,
        sum(d.after or 0.0 for d in rest) if sums else None,
        other_delta,
        mix=sum(d.mix or 0.0 for d in rest) if rest and rest[0].mix is not None else None,
        rate=sum(d.rate or 0.0 for d in rest) if rest and rest[0].rate is not None else None,
        share=other_delta / total_delta if total_delta else None,
    )
    return shown, other


def explain(
    runner: Runner,
    specs: dict[str, MetricSpec | str],
    metric: str,
    before: Period,
    after: Period,
    dimensions: list[str],
    top: int = TOP_MEMBERS,
    group_bys: GroupBys | None = None,
) -> Explanation:
    """The change of `metric` between two periods and who drove it, per dimension.

    With `group_bys`, a dimension the metric lacks is split through a same-total metric
    (D113); without it every dimension is queried on the metric itself.
    """
    spec = spec_for(specs, metric)
    sources: dict[str, str] = {}
    for dimension in dimensions:
        source = source_metric(spec, dimension, group_bys)
        if source is None:
            assert group_bys is not None
            raise ValueError(f"{metric!r} cannot be split by {dimension!r}. Available: {', '.join(sorted(reachable(spec, group_bys)))}")
        sources[dimension] = source
    parts = [spec.numerator, spec.denominator] if spec.kind == "ratio" else [metric]
    names = [part for part in parts if part]
    total0, total1 = _total(runner, names, before), _total(runner, names, after)
    if spec.kind == "ratio":
        assert spec.numerator and spec.denominator
        if not total0[spec.denominator] or not total1[spec.denominator]:
            raise ValueError(f"{metric!r}: no {spec.denominator} in one of the periods, the ratio is undefined")
        value0 = total0[spec.numerator] / total0[spec.denominator]
        value1 = total1[spec.numerator] / total1[spec.denominator]
    else:
        value0, value1 = total0[metric], total1[metric]
    result = Explanation(metric, spec.kind, before.label, after.label, value0, value1, value1 - value0, _relative(value0, value1))

    for source in sorted(set(sources.values()) - {metric}):
        # the promise of same_total_as, checked on the periods asked about
        for period, value in ((before, value0), (after, value1)):
            total = _total(runner, [source], period)[source]
            if abs(total - value) > SAME_TOTAL_TOLERANCE:
                raise ValueError(f"{source!r} ({total:,.2f}) does not match {metric!r} ({value:,.2f}) in {period.label}; cannot split through it")

    for dimension in dimensions:
        source = sources[dimension]
        if source != metric:  # both additive (specs_from_manifest)
            m0 = {m: v[source] for m, v in _by_member(runner([source], [dimension], before.start, before.end), dimension, [source]).items()}
            m1 = {m: v[source] for m, v in _by_member(runner([source], [dimension], after.start, after.end), dimension, [source]).items()}
            shown, other = additive_drivers(m0, m1, result.delta, top)
            covered = abs(sum(d.delta for d in shown) / result.delta) if result.delta else 0.0
            result.breakdowns.append(Breakdown(dimension, shown, other, len(m0.keys() | m1.keys()), covered, via=source))
            continue
        rows0, rows1 = runner(names, [dimension], before.start, before.end), runner(names, [dimension], after.start, after.end)
        if spec.kind == "ratio":
            assert spec.numerator and spec.denominator
            pair = [spec.numerator, spec.denominator]
            t0 = _by_member(rows0, dimension, pair)
            t1 = _by_member(rows1, dimension, pair)
            shown, other = ratio_drivers(
                {m: (v[pair[0]], v[pair[1]]) for m, v in t0.items()},
                {m: (v[pair[0]], v[pair[1]]) for m, v in t1.items()},
                value0,
                result.delta,
                top,
            )
            members = len(t0.keys() | t1.keys())
        else:
            m0 = {m: v[metric] for m, v in _by_member(rows0, dimension, [metric]).items()}
            m1 = {m: v[metric] for m, v in _by_member(rows1, dimension, [metric]).items()}
            shown, other = additive_drivers(m0, m1, result.delta, top)
            members = len(m0.keys() | m1.keys())
        covered = abs(sum(d.delta for d in shown) / result.delta) if result.delta else 0.0
        result.breakdowns.append(Breakdown(dimension, shown, other, members, covered))
    return result


def moved_past_threshold(before: float, after: float, base: float, threshold: float = DEFAULT_THRESHOLD, min_base: float = DEFAULT_MIN_BASE) -> bool:
    """A change counts when |relative change| >= threshold and the earlier period had at least `min_base` orders."""
    relative = _relative(before, after)
    return relative is not None and base >= min_base and abs(relative) >= threshold


def to_dict(explanation: Explanation) -> dict[str, Any]:
    return asdict(explanation)


# --- the run email -----------------------------------------------------------------------------


def previous_month(month: datetime.date) -> datetime.date:
    return (month.replace(day=1) - datetime.timedelta(days=1)).replace(day=1)


def month_period(first_day: datetime.date) -> Period:
    next_first = (first_day.replace(day=28) + datetime.timedelta(days=4)).replace(day=1)
    return Period(first_day, next_first - datetime.timedelta(days=1))


def run_check(
    runner: Runner,
    specs: dict[str, MetricSpec | str],
    latest_month: datetime.date,
    threshold: float = DEFAULT_THRESHOLD,
    min_base: float = DEFAULT_MIN_BASE,
    metrics: list[str] = EMAIL_METRICS,
    dimensions: list[str] = EMAIL_DIMENSIONS,
    group_bys: GroupBys | None = None,
) -> dict[str, Any]:
    """The latest month against the one before it: every watched metric, with drivers for those that moved.

    With `group_bys`, each metric is split only by the dimensions it reaches (a rate of orders
    has no product category).
    """
    after, before = month_period(latest_month), month_period(previous_month(latest_month))
    base = _total(runner, [BASE_METRIC], before)[BASE_METRIC]
    checked: list[dict[str, Any]] = []
    moved: list[dict[str, Any]] = []
    for metric in metrics:
        spec = spec_for(specs, metric)
        parts = [spec.numerator, spec.denominator] if spec.kind == "ratio" else [metric]
        names = [p for p in parts if p]
        t0, t1 = _total(runner, names, before), _total(runner, names, after)
        if spec.kind == "ratio":
            assert spec.numerator and spec.denominator
            if not t0[spec.denominator] or not t1[spec.denominator]:
                continue
            v0, v1 = t0[spec.numerator] / t0[spec.denominator], t1[spec.numerator] / t1[spec.denominator]
        else:
            v0, v1 = t0[metric], t1[metric]
        hit = moved_past_threshold(v0, v1, base, threshold, min_base)
        checked.append({"metric": metric, "before": v0, "after": v1, "relative": _relative(v0, v1), "moved": hit})
        if hit:
            usable = [d for d in dimensions if source_metric(spec, d, group_bys) is not None]
            explanation = explain(runner, specs, metric, before, after, usable, group_bys=group_bys)
            # the most telling dimensions: a few members cover much of the change
            explanation.breakdowns.sort(key=lambda b: -b.informative)
            del explanation.breakdowns[EMAIL_DIMENSION_COUNT:]
            moved.append(to_dict(explanation))
    return {
        "before_period": before.label,
        "after_period": after.label,
        "threshold": threshold,
        "min_base": min_base,
        "base": base,
        "checked": checked,
        "moved": moved,
    }


def _fmt(value: float | None, metric: str) -> str:
    if value is None:
        return "-"
    if metric.endswith("_rate"):
        return f"{value:.1%}"
    return f"{value:,.2f}" if abs(value) < 100 and value != int(value) else f"{value:,.0f}"


def _signed(value: float, metric: str) -> str:
    if metric.endswith("_rate"):
        points = round(value * 100, 1) + 0.0  # no "-0.0"
        return f"{points:+.1f} pp"
    text = _fmt(abs(value), metric)
    return ("-" if value < 0 else "+") + text


def drivers_html(drivers: dict[str, Any]) -> str:
    """The email block: per metric that moved, the top members per dimension."""
    if not drivers.get("moved"):
        return ""
    parts = [
        email_html.heading(f"Metrics that moved: {drivers['after_period']} vs {drivers['before_period']}"),
        email_html.paragraph(
            f"Change of at least {drivers['threshold']:.0%} against a month with {drivers['base']:,.0f} orders. "
            "Each block splits the change by one dimension; the rows add up to the total change."
        ),
    ]
    for item in drivers["moved"]:
        metric = item["metric"]
        relative = f" ({item['relative']:+.1%})" if item["relative"] is not None else ""
        parts.append(
            email_html.heading(
                f"{metric}: {_fmt(item['before'], metric)} to {_fmt(item['after'], metric)}, {_signed(item['delta'], metric)}{relative}"
            )
        )
        for breakdown in item["breakdowns"]:
            ratio = item["kind"] == "ratio"
            lines = []
            for driver in [*breakdown["drivers"], *([breakdown["other"]] if breakdown["other"] else [])]:
                cells = [escape(driver["member"]), _signed(driver["delta"], metric)]
                if ratio:
                    cells += [_signed(driver["mix"], metric) if driver["mix"] is not None else "", _signed(driver["rate"], metric) if driver["rate"] is not None else ""]
                cells.append(f"{driver['share']:.0%}" if driver["share"] is not None else "")
                lines.append(cells)
            via = f" (via {breakdown['via']})" if breakdown.get("via") else ""
            header = ["by " + breakdown["dimension"] + via, "change"] + (["mix", "rate"] if ratio else []) + ["share"]
            parts.append(_table(header, lines))
    return "".join(parts)


def _table(header: list[str], rows: list[list[str]]) -> str:
    font = email_html.FONT
    head = "".join(
        f'<th style="padding:4px 10px;text-align:{"left" if i == 0 else "right"};font:600 12px {font};color:{email_html.MUTED};'
        f'border-bottom:1px solid {email_html.BORDER}">{escape(h)}</th>'
        for i, h in enumerate(header)
    )
    body = "".join(
        "<tr>"
        + "".join(
            f'<td style="padding:4px 10px;text-align:{"left" if i == 0 else "right"};font:13px {font};color:{email_html.TEXT};'
            f'border-bottom:1px solid {email_html.BORDER}">{cell}</td>'
            for i, cell in enumerate(row)
        )
        + "</tr>"
        for row in rows
    )
    return f'<table role="presentation" cellspacing="0" style="border-collapse:collapse;margin:4px 0 12px;width:100%"><tr>{head}</tr>{body}</table>'


def subject_hint(drivers: dict[str, Any] | None) -> str:
    """', revenue -12%' appended to the OK subject; empty when nothing moved."""
    if not drivers or not drivers.get("moved"):
        return ""
    return "".join(f", {m['metric']} {m['relative']:+.0%}" for m in drivers["moved"] if m["relative"] is not None)


# --- MetricFlow --------------------------------------------------------------------------------


def metricflow_runner(engine: Any) -> Runner:
    """A Runner over a MetricFlowEngine; the engine's target decides the warehouse."""
    from metricflow.engine.metricflow_engine import MetricFlowQueryRequest

    def run(metrics: list[str], group_by: list[str], start: datetime.date, end: datetime.date) -> list[dict[str, Any]]:
        result = engine.query(
            MetricFlowQueryRequest.create(
                metric_names=metrics,
                group_by_names=group_by or None,
                time_constraint_start=datetime.datetime.combine(start, datetime.time.min),
                time_constraint_end=datetime.datetime.combine(end, datetime.time.min),
            )
        )
        table = result.result_df
        if table is None:
            raise ValueError("MetricFlow returned no result table")
        return [dict(zip(table.column_names, row, strict=True)) for row in table.rows]

    return run


def metricflow_group_bys(engine: Any) -> GroupBys:
    """What a metric can be split by in MetricFlow: categorical dimensions and the SPLIT_ENTITIES."""
    cache: dict[str, set[str]] = {}

    def group_bys(metric: str) -> set[str]:
        if metric not in cache:
            names: set[str] = set()
            for item in engine.list_group_bys(metric_names=[metric]):
                if hasattr(item, "dunder_name"):  # a Dimension
                    if item.type.value == "categorical":
                        names.add(item.dunder_name)
                elif item.name in SPLIT_ENTITIES:
                    names.add(item.name)
            cache[metric] = names
        return cache[metric]

    return group_bys


def load_engine(target: str, project_dir: Path, manifest_path: Path, profiles_dir: Path | None = None) -> Any:
    """MetricFlow's engine on a dbt target, over the semantic manifest at `manifest_path`.

    Not `CLIConfiguration().mf`: that one reads `<project>/target/semantic_manifest.json`, a
    hard-coded path that ignores DBT_TARGET_PATH (#269). The caller parses the project with the
    warehouse's env into its own target dir, so the manifest names the right catalog / dataset.
    stdout stays untouched for the caller.
    """
    os.environ["DBT_TARGET"] = target
    with contextlib.redirect_stdout(sys.stderr):
        from dbt.adapters.factory import get_adapter_by_type
        from dbt_metricflow.cli.dbt_connectors.adapter_backed_client import AdapterBackedSqlClient
        from dbt_metricflow.cli.dbt_connectors.dbt_config_accessor import dbtProjectMetadata
        from metricflow.engine.metricflow_engine import MetricFlowEngine
        from metricflow_semantics.model.dbt_manifest_parser import parse_manifest_from_dbt_generated_manifest
        from metricflow_semantics.model.semantic_manifest_lookup import SemanticManifestLookup

        # `dbt debug` inside registers the adapter for the target's profile
        metadata = dbtProjectMetadata.load_from_paths(profiles_path=profiles_dir or project_dir, project_path=project_dir)
        manifest = parse_manifest_from_dbt_generated_manifest(manifest_json_string=manifest_path.read_text(encoding="utf-8"))
        adapter: Any = get_adapter_by_type(metadata.profile.credentials.type)  # a BaseAdapter; typed as the protocol
        return MetricFlowEngine(semantic_manifest_lookup=SemanticManifestLookup(manifest), sql_client=AdapterBackedSqlClient(adapter))


def latest_month(runner: Runner) -> datetime.date:
    """First day of the month of the newest order."""
    rows = runner(["orders"], ["metric_time__month"], datetime.date(2000, 1, 1), datetime.date(2040, 12, 31))
    months = [r["metric_time__month"] for r in rows if _number(r["orders"]) > 0]
    if not months:
        raise ValueError("no orders")
    newest = max(months)
    return newest.date() if isinstance(newest, datetime.datetime) else newest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--target", required=True, help="dbt target to query")
    parser.add_argument("--project-dir", type=Path, default=Path(__file__).resolve().parents[1] / "transform")
    parser.add_argument("--month", help="YYYY-MM to check (default: the month of the newest order)")
    parser.add_argument("--threshold", type=float, default=DEFAULT_THRESHOLD)
    parser.add_argument("--min-base", type=float, default=DEFAULT_MIN_BASE)
    args = parser.parse_args(argv)

    # The same file for MetricFlow and the specs: the one `dbt parse` wrote for this target
    manifest_path = Path(os.environ.get("DBT_TARGET_PATH") or args.project_dir / "target") / "semantic_manifest.json"
    engine = load_engine(args.target, args.project_dir, manifest_path)
    runner = metricflow_runner(engine)
    specs = specs_from_manifest(json.loads(manifest_path.read_text(encoding="utf-8")))
    with contextlib.redirect_stdout(sys.stderr):
        month = datetime.date.fromisoformat(args.month + "-01") if args.month else latest_month(runner)
        result = run_check(runner, specs, month, args.threshold, args.min_base, group_bys=metricflow_group_bys(engine))
    print(json.dumps(result, default=str, allow_nan=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())

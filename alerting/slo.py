"""Service level objectives, error budget and burn rate (D111).

Pure arithmetic over daily event counts, shared by the SLO report
(.github/scripts/slo_report.py, page /slo/) and the burn-rate email
(.github/scripts/notify_slo.py). Stdlib only: the email job runs on the runner's
plain Python.

An SLO is a target share of good events (`0.99`) over a window (28 days). The error
budget is the share of bad events the target allows: `(1 - target) * total` events.
The burn rate says how fast it is spent: the bad share over a short window (7 days)
divided by the allowed share. 1.0 spends the budget exactly over the window, 2.0
twice as fast. A window with no events has no SLI (not 100%): `state` is `no data`.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from typing import Any

from . import email_html

Daily = list[tuple[date, int, int]]  # (day, good events, total events)


@dataclass(frozen=True)
class Status:
    key: str
    title: str
    target: float
    window_days: int
    good: int
    total: int
    burn_days: int
    burn_good: int
    burn_total: int
    sli: float | None
    allowed_bad: float  # error budget of the window, in events
    bad: int
    budget_left: float | None  # share of the budget left, below 0 when overspent
    burn_rate: float | None  # over `burn_days`
    state: str  # ok | burning | exhausted | no data
    daily: list[dict[str, Any]] = field(default_factory=list)

    def as_dict(self, daily: bool = True) -> dict[str, Any]:
        data = asdict(self)
        if not daily:
            del data["daily"]
        return data


def evaluate(
    key: str,
    title: str,
    target: float,
    events: Daily,
    today: date,
    window_days: int = 28,
    burn_days: int = 7,
    burn_alert: float = 2.0,
) -> Status:
    """Status of one SLO over the `window_days` ending on `today` (inclusive)."""
    if not 0 < target < 1:
        raise ValueError(f"target of {key} must be between 0 and 1 (exclusive), got {target}")
    first = today - timedelta(days=window_days - 1)
    burn_first = today - timedelta(days=burn_days - 1)
    by_day: dict[date, tuple[int, int]] = {}
    for day, good, total in events:
        if first <= day <= today:
            old_good, old_total = by_day.get(day, (0, 0))
            by_day[day] = (old_good + good, old_total + total)
    good = sum(g for g, _ in by_day.values())
    total = sum(t for _, t in by_day.values())
    burn_good = sum(g for d, (g, _) in by_day.items() if d >= burn_first)
    burn_total = sum(t for d, (_, t) in by_day.items() if d >= burn_first)
    allowed = round((1 - target) * total, 9)  # float noise must not flip `bad > allowed`
    bad = total - good
    burn_rate = round((burn_total - burn_good) / burn_total / (1 - target), 9) if burn_total else None
    budget_left = 1 - bad / allowed if allowed else None
    if not total:
        state = "no data"
    elif bad > allowed:
        state = "exhausted"
    elif burn_rate is not None and burn_rate >= burn_alert:
        state = "burning"
    else:
        state = "ok"
    daily = [
        {
            "day": (first + timedelta(days=i)).isoformat(),
            "good": by_day.get(first + timedelta(days=i), (0, 0))[0],
            "total": by_day.get(first + timedelta(days=i), (0, 0))[1],
        }
        for i in range(window_days)
    ]
    return Status(
        key=key, title=title, target=target, window_days=window_days,
        good=good, total=total, burn_days=burn_days, burn_good=burn_good, burn_total=burn_total,
        sli=good / total if total else None, allowed_bad=allowed, bad=bad,
        budget_left=budget_left, burn_rate=burn_rate, state=state, daily=daily,
    )


def alerts(statuses: list[Status]) -> list[Status]:
    """The SLOs that call for an email: budget spent, or burning at the alert rate."""
    return [s for s in statuses if s.state in ("burning", "exhausted")]


def percent(share: float | None, digits: int = 1) -> str:
    return "n/a" if share is None else f"{share * 100:.{digits}f}%"


def describe(s: dict[str, Any]) -> str:
    """One finding as text, from a Status.as_dict()."""
    burn = s["burn_rate"]
    burn_text = "no events" if burn is None else f"{float(burn):.1f}x"
    return (
        f"{s['title']}: SLI {percent(s['sli'])} over {s['window_days']} days "
        f"(target {percent(float(s['target']), 0)}), {s['bad']} bad of {s['total']} events; "
        f"burn rate {burn_text} over the last {s['burn_days']} days; "
        f"error budget {'spent' if s['state'] == 'exhausted' else percent(s['budget_left'], 0) + ' left'}"
    )


def email(
    flagged: list[dict[str, Any]],
    prefix: str,
    links: list[tuple[str, str]],
    footer: str,
    burn_alert: float = 2.0,
) -> tuple[str, str] | None:
    """(subject, html) for the flagged SLOs (Status.as_dict()), or None when none is."""
    if not flagged:
        return None
    names = ", ".join(str(s["title"]).lower() for s in flagged)
    title = f"Error budget burning: {names}"
    items = [
        (
            "spent" if s["state"] == "exhausted" else f"{float(s['burn_rate']):.1f}x",
            describe(s),
            f"slo {s['key']}",
        )
        for s in flagged
    ]
    intro = (
        "The marts are missing their service level objectives faster than the error budget "
        f"allows: a burn rate of {burn_alert:g} or more over the short window, or a budget "
        "spent within the window (28 days)."
    )
    body = email_html.findings(items, "SLO") + email_html.heading("How to read this") + email_html.paragraph(
        "Error budget: the bad events the target allows in the window. Burn rate: the bad share "
        "over the short window divided by the allowed share; 1x spends the budget exactly over "
        "the window. The SLO page has the definitions and the daily history."
    )
    html = email_html.page("SLO", title, intro, body, links=links, footer=footer)
    return f"{prefix} SLO: {title}", html

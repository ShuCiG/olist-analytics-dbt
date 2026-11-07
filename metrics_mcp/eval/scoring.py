"""Grades an agent's final answer against the gold rows. No warehouse, no agent: plain pytest covers it."""

import json
import math
from typing import Any

# Enough for an agent that copies a number and rounds it to two decimals; rates need the digits
TOLERANCE = 0.005


def parse_final_line(text: str) -> dict[str, Any] | None:
    """The JSON object on the last non-empty line, or None."""
    lines = [line.strip().strip("`") for line in text.splitlines() if line.strip().strip("`")]
    if not lines:
        return None
    try:
        parsed = json.loads(lines[-1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _close(answer: object, gold: float) -> bool:
    if isinstance(answer, bool) or not isinstance(answer, int | float):
        return False
    return math.isclose(answer, gold, rel_tol=TOLERANCE, abs_tol=TOLERANCE / 100)


def normalize_key(key: object) -> str:
    """Grain-independent key: `2018-01-01T00:00:00` and `2018-01-01` match, as do `True` and `true`."""
    text = str(key).strip().lower()
    if text.endswith(".0"):
        text = text[:-2]
    return text.removesuffix("t00:00:00").removesuffix(" 00:00:00")


def period_key(key: object, group_by: str) -> str:
    """A period at the grain asked for: `2017-11` and `2017-11-01` are the same month."""
    text = normalize_key(key)
    for suffix, length in (("__year", 4), ("__month", 7)):
        if group_by.endswith(suffix):
            return text[:length]
    return text


def _mapping(gold_rows: list[dict[str, str]], key: str, metric: str) -> dict[str, float]:
    return {period_key(row[key], key): float(row[metric]) for row in gold_rows if row[metric] != ""}


def grade(
    question: dict[str, Any], gold_rows: list[dict[str, str]] | None, final: dict[str, Any] | None, tool_calls: int
) -> tuple[bool, str]:
    """(correct, reason). `gold_rows` is None for a question the agent must refuse.

    No tool call is not right: a number would come from memory, and a refusal without looking at
    the catalog (or because the server failed to connect) says nothing about the semantic layer.
    The exception is `refuse_without_lookup`, a request no metric could serve, like running SQL.
    """
    if final is None:
        return False, "no parseable final line"
    if tool_calls == 0 and not question.get("refuse_without_lookup"):
        return False, "no tool call"
    refused = final.get("refused") is True
    if question.get("refuse"):
        return (True, "refused") if refused else (False, "answered a question it must refuse")
    if refused:
        return False, "refused an answerable question"
    answer = final.get("answer")
    assert gold_rows is not None

    gold = question["gold"]
    metric = gold["metrics"][0]
    if question["shape"] == "scalar":
        return (True, "match") if _close(answer, float(gold_rows[0][metric])) else (False, f"{answer} != {gold_rows[0][metric]}")

    group_by = gold["group_by"][0]
    expected = _mapping(gold_rows, group_by, metric)
    if question["shape"] == "top_key":
        top = max(expected, key=lambda key: expected[key])
        return (True, "match") if period_key(answer, group_by) == top else (False, f"{answer} != {top}")

    if not isinstance(answer, dict):
        return False, "expected an object of key to number"
    got = {period_key(key, group_by): value for key, value in answer.items()}
    if got.keys() != expected.keys():
        return False, f"keys {sorted(got)} != {sorted(expected)}"
    wrong = [key for key, value in expected.items() if not _close(got[key], value)]
    return (False, f"values differ at {wrong}") if wrong else (True, "match")

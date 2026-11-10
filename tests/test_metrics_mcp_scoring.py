"""Grading of the MCP accuracy evaluation; a lenient grader would inflate the published result."""

from typing import Any

import pytest

from metrics_mcp.eval.scoring import difference, grade, normalize_key, parse_final_line, period_key

SCALAR = {"shape": "scalar", "gold": {"metrics": ["revenue"]}}
MAPPING = {"shape": "mapping", "gold": {"metrics": ["revenue"], "group_by": ["customer__state"]}}
TOP_KEY = {"shape": "top_key", "gold": {"metrics": ["revenue"], "group_by": ["customer__state"]}}
REFUSE: dict[str, Any] = {"refuse": True}
STATES = [{"customer__state": "SP", "revenue": "100.0"}, {"customer__state": "RJ", "revenue": "50.0"}]


def test_final_line_after_prose() -> None:
    assert parse_final_line('The answer is below.\n\n{"answer": 1.5}\n') == {"answer": 1.5}


def test_final_line_in_a_code_fence() -> None:
    assert parse_final_line('```\n{"answer": 1.5}\n```') == {"answer": 1.5}


@pytest.mark.parametrize("text", ["", "no json here", '{"answer": 1}\nand more text', "[1, 2]"])
def test_final_line_must_be_a_json_object(text: str) -> None:
    assert parse_final_line(text) is None


def test_scalar_within_tolerance() -> None:
    assert grade(SCALAR, [{"revenue": "100.0"}], {"answer": 100.4}, 1)[0]
    assert not grade(SCALAR, [{"revenue": "100.0"}], {"answer": 101}, 1)[0]


def test_percent_instead_of_fraction_is_wrong() -> None:
    assert not grade(SCALAR, [{"revenue": "0.08"}], {"answer": 8}, 1)[0]


@pytest.mark.parametrize("answer", ["100", True, None, [100]])
def test_scalar_must_be_a_number(answer: object) -> None:
    assert not grade(SCALAR, [{"revenue": "1"}], {"answer": answer}, 1)[0]


def test_mapping_needs_every_key_and_value() -> None:
    assert grade(MAPPING, STATES, {"answer": {"SP": 100.0, "RJ": 50.0}}, 1)[0]
    assert not grade(MAPPING, STATES, {"answer": {"SP": 100.0}}, 1)[0]
    assert not grade(MAPPING, STATES, {"answer": {"SP": 100.0, "RJ": 60.0}}, 1)[0]
    assert not grade(MAPPING, STATES, {"answer": 150.0}, 1)[0]


def test_top_key() -> None:
    assert grade(TOP_KEY, STATES, {"answer": "SP"}, 1)[0]
    assert not grade(TOP_KEY, STATES, {"answer": "RJ"}, 1)[0]


def test_keys_ignore_time_format_and_case() -> None:
    assert normalize_key("2018-01-01T00:00:00") == normalize_key("2018-01-01") == normalize_key("2018-01-01 00:00:00")
    assert normalize_key(True) == normalize_key("true")
    assert normalize_key(1.0) == normalize_key("1")


def test_refusal_is_right_only_where_expected() -> None:
    assert grade(REFUSE, None, {"refused": True, "reason": "no such metric"}, 1)[0]
    assert not grade(REFUSE, None, {"answer": 0.2}, 1)[0]
    assert not grade(SCALAR, [{"revenue": "1"}], {"refused": True}, 1)[0]


def test_no_tool_call_is_not_right() -> None:
    assert not grade(REFUSE, None, {"refused": True}, 0)[0]
    assert not grade(SCALAR, [{"revenue": "1"}], {"answer": 1}, 0)[0]


def test_request_no_metric_could_serve_needs_no_lookup() -> None:
    question = {"refuse": True, "refuse_without_lookup": True}
    assert grade(question, None, {"refused": True}, 0)[0]
    assert not grade(question, None, {"answer": 5}, 0)[0]


def test_period_at_the_grain_asked_for() -> None:
    assert period_key("2017-11", "metric_time__month") == period_key("2017-11-01 00:00:00", "metric_time__month")
    assert period_key("2016", "metric_time__year") == period_key("2016-01-01", "metric_time__year")
    assert period_key("2017-11-02", "metric_time__day") != period_key("2017-11-01", "metric_time__day")


def test_a_different_period_stays_different() -> None:
    gold = [{"metric_time__month": "2017-11-01 00:00:00", "revenue": "9"}, {"metric_time__month": "2017-12-01 00:00:00", "revenue": "1"}]
    question = {"shape": "top_key", "gold": {"metrics": ["revenue"], "group_by": ["metric_time__month"]}}
    assert grade(question, gold, {"answer": "2017-11"}, 1)[0]
    assert not grade(question, gold, {"answer": "2017-12"}, 1)[0]


def test_no_final_line_is_wrong() -> None:
    assert not grade(SCALAR, [{"revenue": "1"}], None, 1)[0]


def test_gold_for_a_change_is_after_minus_before_per_member() -> None:
    before = [{"customer__state": "SP", "revenue": "100"}, {"customer__state": "RJ", "revenue": "50"}]
    after = [{"customer__state": "SP", "revenue": "160"}, {"customer__state": "AC", "revenue": "3"}]
    rows = difference(before, after, "revenue", "customer__state")
    assert {row["customer__state"]: float(row["revenue"]) for row in rows} == {"SP": 60, "RJ": -50, "AC": 3}
    assert [float(row["revenue"]) for row in difference([{"revenue": "10"}], [{"revenue": ""}], "revenue", None)] == [-10]

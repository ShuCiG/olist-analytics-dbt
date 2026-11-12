"""The MCP server's argument checks: nothing an agent sends may become free SQL."""

import datetime
import decimal

import pytest
from pydantic import ValidationError

from metrics_mcp import query

DIMENSIONS = {"customer__state", "order__is_delivered", "cohort__months_since_first_order", "cohort__cohort_month"}
GRAINS = {"cohort__cohort_month": "month"}


def where(dimension: str, operator: str, value: object) -> str:
    condition = query.Filter.model_validate({"dimension": dimension, "operator": operator, "value": value})
    return query.where_constraint(condition, DIMENSIONS, GRAINS)


def test_categorical_filter() -> None:
    assert where("customer__state", "=", "SP") == "{{ Dimension('customer__state') }} = 'SP'"


def test_list_filter() -> None:
    assert where("customer__state", "in", ["SP", "RJ"]) == "{{ Dimension('customer__state') }} in ('SP', 'RJ')"


def test_boolean_and_number_stay_unquoted() -> None:
    assert where("order__is_delivered", "=", True) == "{{ Dimension('order__is_delivered') }} = true"
    assert where("cohort__months_since_first_order", "<=", 3) == "{{ Dimension('cohort__months_since_first_order') }} <= 3"


def test_time_dimension_names_its_grain() -> None:
    assert where("cohort__cohort_month", ">=", "2018-01-01") == "{{ TimeDimension('cohort__cohort_month', 'month') }} >= '2018-01-01'"


def test_quote_in_a_value_cannot_end_the_string() -> None:
    assert where("customer__state", "=", "x' or 1=1 --") == "{{ Dimension('customer__state') }} = 'x'' or 1=1 --'"


@pytest.mark.parametrize("value", ["{{ 7*7 }}", "{% raw %}", "a}b"])
def test_jinja_in_a_value_is_refused(value: str) -> None:
    with pytest.raises(ValueError, match="braces"):
        where("customer__state", "=", value)


@pytest.mark.parametrize("dimension", ["no_such_dimension", "customer__state) = 1 or (1", "(select 1)"])
def test_dimension_must_belong_to_the_metrics(dimension: str) -> None:
    with pytest.raises(ValueError, match="Unknown dimension"):
        where(dimension, "=", "x")


@pytest.mark.parametrize("operator", ["like", "= 1 or 1 =", "; drop table x;"])
def test_operator_comes_from_a_closed_list(operator: str) -> None:
    with pytest.raises(ValidationError):
        where("customer__state", operator, "x")


def test_filter_takes_no_extra_field() -> None:
    with pytest.raises(ValidationError):
        query.Filter.model_validate({"dimension": "customer__state", "operator": "=", "value": "SP", "sql": "1=1"})


@pytest.mark.parametrize(("operator", "value"), [("in", "SP"), ("in", []), ("=", ["SP"])])
def test_operator_and_value_shape_must_agree(operator: str, value: object) -> None:
    with pytest.raises(ValueError, match="value"):
        where("customer__state", operator, value)


def test_unknown_metric_lists_the_real_ones() -> None:
    with pytest.raises(ValueError, match="Available: orders, revenue"):
        query.check_metrics(["profit"], {"revenue", "orders"})


def test_no_metric_is_refused() -> None:
    with pytest.raises(ValueError, match="at least one"):
        query.check_metrics([], {"revenue"})


@pytest.mark.parametrize("name", ["customer__state; drop", "(select 1)", "A", ""])
def test_group_by_is_a_plain_name(name: str) -> None:
    with pytest.raises(ValueError, match="Invalid group by"):
        query.check_group_by([name])


def test_order_by_names_a_queried_column() -> None:
    query.check_order_by(["-revenue", "customer__state"], ["revenue"], ["customer__state"])
    with pytest.raises(ValueError, match="Order by"):
        query.check_order_by(["orders"], ["revenue"], [])


@pytest.mark.parametrize("limit", [0, -1, query.MAX_ROWS + 1])
def test_limit_range(limit: int) -> None:
    with pytest.raises(ValueError, match="Limit"):
        query.check_limit(limit)


def test_json_value() -> None:
    assert query.json_value(decimal.Decimal("1.5")) == 1.5
    assert query.json_value(datetime.datetime(2018, 1, 1)) == "2018-01-01T00:00:00"
    assert query.json_value(datetime.date(2018, 1, 1)) == "2018-01-01"
    assert query.json_value("SP") == "SP"


@pytest.mark.parametrize("name", ["customer", "order__customer", "order_item__customer", "payment__customer"])
def test_person_entity_is_refused_as_group_by(name: str) -> None:
    # MetricFlow takes an entity as a group by: `customer` returns customer_unique_id values (#281)
    with pytest.raises(ValueError, match="person id"):
        query.check_group_by([name])


def test_person_entity_is_refused_as_filter() -> None:
    condition = query.Filter.model_validate({"dimension": "customer", "operator": "=", "value": "ef1d0f19"})
    with pytest.raises(ValueError, match="person id"):
        query.where_constraint(condition, {*DIMENSIONS, "customer"}, GRAINS)


@pytest.mark.parametrize("name", ["customer__state", "customer__is_repeat_customer", "customer__first_order_at__month", "seller", "product"])
def test_dimensions_of_a_person_and_other_entities_stay_open(name: str) -> None:
    query.check_group_by([name])

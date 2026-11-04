"""One defect per scenario. Each corrupts the clean dump's last month, the new data a
feed would deliver next, and names what must react.

A reaction is `loader` (the load stopped), `schema:new:<table>` / `schema:removed:<table>`
(feed status, sent as the SCHEMA email), `contract` (input data contract failed) or
`<status>:<dbt test name prefix>` with status fail, warn or error. Other reactions may
come on top. No expected reaction means a recorded gap: nothing at all may react, and
`pinned` shows what the data looks like then. A test added later turns the gap's
scenario red until the scenario and the README table say what catches it now.
"""

from collections.abc import Callable
from dataclasses import dataclass

from fault_injection.dump import Rows

NEW_MONTH = "2018-06"
FIRST_ORDER_OF_LOADED_MONTH = "order00601"  # 2018-05-01


def new_order_ids(rows: Rows) -> list[str]:
    return [r["order_id"] for r in rows["orders"] if r["order_purchase_timestamp"].startswith(NEW_MONTH)]


def of_new_orders(rows: Rows, table: str) -> list[dict[str, str]]:
    ids = set(new_order_ids(rows))
    return [r for r in rows[table] if r["order_id"] in ids]


def clean_month(rows: Rows) -> None:
    pass


def duplicated_orders(rows: Rows) -> None:
    rows["orders"].extend(dict(r) for r in of_new_orders(rows, "orders")[:5])


def conflicting_duplicate_orders(rows: Rows) -> None:
    rows["orders"].extend({**r, "order_status": "canceled"} for r in of_new_orders(rows, "orders")[:5])


def duplicated_customers(rows: Rows) -> None:
    rows["customers"].extend(dict(r) for r in rows["customers"][:5])


def payments_times_100(rows: Rows) -> None:
    for r in of_new_orders(rows, "order_payments"):
        r["payment_value"] = f"{float(r['payment_value']) * 100:.2f}"


def prices_times_100(rows: Rows) -> None:
    for r in of_new_orders(rows, "order_items"):
        r["price"] = f"{float(r['price']) * 100:.2f}"


def delivered_before_purchase(rows: Rows) -> None:
    for r in of_new_orders(rows, "orders")[:5]:
        r["order_delivered_customer_date"] = "2018-01-01 00:00:00"


def orphan_customers(rows: Rows) -> None:
    for r in of_new_orders(rows, "orders")[:5]:
        r["customer_id"] = "customer_nobody"


def orphan_products(rows: Rows) -> None:
    for r in of_new_orders(rows, "order_items")[:5]:
        r["product_id"] = "product_nobody"


def new_source_column(rows: Rows) -> None:
    for r in rows["orders"]:
        r["order_channel"] = "web"


def removed_source_column(rows: Rows) -> None:
    for r in rows["orders"]:
        del r["order_approved_at"]


def required_column_removed(rows: Rows) -> None:
    for r in rows["orders"]:
        del r["customer_id"]


def late_status_change(rows: Rows) -> None:
    order = next(r for r in rows["orders"] if r["order_id"] == FIRST_ORDER_OF_LOADED_MONTH)
    order["order_status"] = "canceled"


def unknown_status(rows: Rows) -> None:
    for r in of_new_orders(rows, "orders")[:5]:
        r["order_status"] = "refunded"


def feed_mostly_missing(rows: Rows) -> None:
    ids = new_order_ids(rows)
    drop = set(ids) - set(ids[::20])
    for table in ("orders", "order_items", "order_payments", "order_reviews"):
        rows[table] = [r for r in rows[table] if r["order_id"] not in drop]


def impossible_purchase_time(rows: Rows) -> None:
    of_new_orders(rows, "orders")[0]["order_purchase_timestamp"] = "2018-06-31 25:00:00"


def missing_payments(rows: Rows) -> None:
    ids = set(new_order_ids(rows)[:5])
    rows["order_payments"] = [r for r in rows["order_payments"] if r["order_id"] not in ids]


def score_out_of_range(rows: Rows) -> None:
    for r in of_new_orders(rows, "order_reviews")[:5]:
        r["review_score"] = "50"


@dataclass(frozen=True)
class Scenario:
    defect: str  # the row in the README's coverage table
    corrupt: Callable[[Rows], None]
    expected: tuple[str, ...] = ()
    pinned: tuple[str, object] | None = None  # query on the warehouse, its single value
    contract: bool = False  # also run the input data contract (a slow step)

    @property
    def name(self) -> str:
        return self.corrupt.__name__


SCENARIOS = [
    Scenario("No defect (control)", clean_month, contract=True),
    Scenario(
        "Identical duplicate orders",
        duplicated_orders,
        pinned=("select count(*) - count(distinct order_id) from raw.orders", 0),
    ),
    Scenario("Two different rows for one order", conflicting_duplicate_orders, ("loader",)),
    Scenario("Duplicated customers", duplicated_customers, ("fail:unique_stg_olist__customers_customer_id",)),
    Scenario(
        "Payment amounts × 100",
        payments_times_100,
        ("warn:assert_payments_match_order_amount", "warn:elementary_column_anomalies_fct_orders_v2_average__paid_amount"),
    ),
    Scenario("Item prices × 100", prices_times_100, ("warn:assert_payments_match_order_amount",)),
    Scenario(
        "Delivery before purchase",
        delivered_before_purchase,
        ("fail:is_not_before_fct_orders_v2_delivered_to_customer_at__purchased_at",),
    ),
    Scenario(
        "Orders with an unknown customer",
        orphan_customers,
        ("fail:relationships_stg_olist__orders_customer_id",),
    ),
    Scenario(
        "Order lines with an unknown product",
        orphan_products,
        ("fail:relationships_stg_olist__order_items_product_id", "fail:relationships_fct_order_items_product_id"),
    ),
    Scenario(
        "New source column",
        new_source_column,
        ("schema:new:orders", "warn:elementary_source_schema_changes_olist_orders"),
        contract=True,
    ),
    Scenario("Optional source column removed", removed_source_column, ("schema:removed:orders",), contract=True),
    Scenario(
        "Required source column removed",
        required_column_removed,
        ("schema:removed:orders", "fail:not_null_stg_olist__orders_customer_id", "contract"),
        contract=True,
    ),
    Scenario(
        "Late status change of a loaded order",
        late_status_change,
        pinned=(f"select order_status from raw.orders where order_id = '{FIRST_ORDER_OF_LOADED_MONTH}'", "delivered"),
    ),
    Scenario("Unknown order status", unknown_status, ("fail:accepted_values_stg_olist__orders_order_status",)),
    Scenario(
        "Feed delivers 5 % of the orders",
        feed_mostly_missing,
        ("warn:elementary_volume_anomalies_fct_orders_v2", "warn:elementary_freshness_anomalies_fct_orders_v2"),
    ),
    Scenario(
        "Well-formed but impossible purchase time",
        impossible_purchase_time,
        ("error:not_null_stg_olist__orders_purchased_at",),
    ),
    Scenario("Orders without payments", missing_payments, ("warn:assert_payments_match_order_amount",)),
    Scenario("Review score out of range", score_out_of_range, ("fail:accepted_values_stg_olist__order_reviews_review_score",)),
]

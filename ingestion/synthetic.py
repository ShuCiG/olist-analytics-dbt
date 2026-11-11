"""Generated order months after the end of the Olist dump: the live feed goes on (D112).

A generated month is a bootstrap of a real one: its orders are drawn, with
replacement, from the latest dump month of the same calendar month that is a full
month (at least 10 % of the busiest month's orders, the rule of
int_order_history_bounded), scaled to the dump's latest year when it is from an
earlier one (Dump.growth). Each drawn order is copied whole: status, items
(products, sellers, prices, freight), payments, reviews (score and text), and every
timestamp moved by the same number of days, the distance between the two months'
first days: a purchase keeps its day of the month and time, and the lags after it
stay exactly as they were. So orders per day, items per order, categories, payment
types, delays and review scores follow the dump, seasonality included (November
gets Black Friday). A dump shorter than a year (the test dumps) may lack the
calendar month: then the latest full month serves, without its days the new month
doesn't have.

What is new per order: `order_id` and `customer_id`. Most orders come from a new
person (new `customer_unique_id`, the template customer's location); a share as in
the dump (customers per person) comes from a person of the dump, with that person's
location. Each month a few sellers of the template month are replaced by new
sellers (new id, the old seller's location) for all their items of that month.
Products are reused, never new.

Reviews keep their template's `review_id`: in Olist `review_id` alone is not unique
(the key is review_id + order_id), and the review label (D92) joins on it, so a
copied text keeps its label without a new label file or an API call.

Seeded per month: a month comes out the same on every run and machine, so the loader
can regenerate every loaded month for the reference snapshots (customers and sellers
are replaced on each load) instead of storing them. Ids are md5 hex strings, like
the dump's, derived from the month and a counter.

Known limits: a generated order arrives complete (its later milestones are in the
first load, the merge never updates it), weekdays are not kept (Black Friday's
peak stays on the 24th), and 29 February never gets orders (the templates of
February are from 2018).
"""

import hashlib
import random
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import cast

import pyarrow as pa
import pyarrow.compute as pc

# Bump on any change to what a month comes out as. The loader regenerates the loaded
# months for the reference snapshots, so a warehouse holding months of another version
# would lose their customers and sellers: the loader refuses it (needs --full-refresh)
VERSION = 2
# Share of the busiest month's orders a month needs to serve as a template
FULL_MONTH_SHARE = 0.1
# Order count of a generated month: the template's, times a factor in this range
VOLUME_JITTER = 0.05
# Share of a month's sellers replaced by new ones
NEW_SELLER_SHARE = 0.02

# Timestamp columns, all moved by one number of days: table -> columns
TIMESTAMPS = {
    "orders": [
        "order_purchase_timestamp",
        "order_approved_at",
        "order_delivered_carrier_date",
        "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "order_items": ["shipping_limit_date"],
    "order_payments": [],
    "order_reviews": ["review_creation_date", "review_answer_timestamp"],
}
ORDER_CHILDREN = ["order_items", "order_payments", "order_reviews"]
TABLES = ["orders", *ORDER_CHILDREN, "customers", "sellers"]


def synthetic_ids(month: str, kind: str, count: int) -> list[str]:
    """Dump-like ids (32 hex): 24 from the month and kind, 8 from a counter; the same on every run."""
    prefix = hashlib.md5(f"olist-synthetic:{month}:{kind}".encode()).hexdigest()[:24]
    return [f"{prefix}{i:08x}" for i in range(count)]


def synthetic_next_month(month: str) -> str:
    """'2018-12' -> '2019-01'."""
    return months_between(month, "9999-12")[0]


def months_between(after: str, until: str) -> list[str]:
    """Months in (after, until], 'YYYY-MM'."""
    months = []
    year, mon = map(int, after.split("-"))
    while True:
        year, mon = (year + 1, 1) if mon == 12 else (year, mon + 1)
        month = f"{year:04d}-{mon:02d}"
        if month > until:
            return months
        months.append(month)


TIME_FORMAT = "%Y-%m-%d %H:%M:%S"


def month_start(month: str) -> date:
    return date(int(month[:4]), int(month[5:]), 1)


def shift_days(column: pa.ChunkedArray, days: int) -> pa.ChunkedArray | pa.Array:
    """'2017-11-24 10:00:00' + 365 days -> '2018-11-24 10:00:00'; null stays null."""
    instants = pc.strptime(column, format=TIME_FORMAT, unit="s")
    shifted = pc.add(instants, pa.scalar(timedelta(days=days), pa.duration("s")))
    return pc.strftime(shifted, format=TIME_FORMAT)


def replace_column(table: pa.Table, name: str, values: pa.Array | pa.ChunkedArray | list[str]) -> pa.Table:
    position = table.schema.get_field_index(name)
    field_ = table.schema.field(position)
    return table.set_column(position, field_, pa.array(values, field_.type) if isinstance(values, list) else values)


def strings(column: pa.ChunkedArray) -> list[str]:
    """A column of ids or timestamps as Python strings (nulls only where the dump has them)."""
    return cast(list[str], column.to_pylist())


def rows_by_key(table: pa.Table, key: str) -> dict[str, list[int]]:
    rows: dict[str, list[int]] = {}
    for i, value in enumerate(strings(table[key])):
        rows.setdefault(value, []).append(i)
    return rows


@dataclass
class Dump:
    """The real dump's tables (strings, as the loader reads them) and lookups into them."""

    tables: dict[str, pa.Table]
    purchased_at: list[str] = field(init=False)
    orders_by_month: dict[str, list[int]] = field(init=False)
    children: dict[str, dict[str, list[int]]] = field(init=False)
    customer_row: dict[str, int] = field(init=False)
    seller_row: dict[str, int] = field(init=False)
    repeat_share: float = field(init=False)

    def __post_init__(self) -> None:
        orders = self.tables["orders"]
        self.purchased_at = strings(orders["order_purchase_timestamp"])
        self.orders_by_month = {}
        for i, purchased_at in enumerate(self.purchased_at):
            if purchased_at:
                self.orders_by_month.setdefault(purchased_at[:7], []).append(i)
        self.children = {name: rows_by_key(self.tables[name], "order_id") for name in ORDER_CHILDREN}
        customers = self.tables["customers"]
        self.customer_row = {cid: i for i, cid in enumerate(strings(customers["customer_id"]))}
        self.seller_row = {sid: i for i, sid in enumerate(strings(self.tables["sellers"]["seller_id"]))}
        people = pc.count_distinct(customers["customer_unique_id"]).as_py()
        self.repeat_share = 1 - people / customers.num_rows if customers.num_rows else 0.0

    @property
    def last_month(self) -> str:
        return max(self.orders_by_month)

    @property
    def full_months(self) -> list[str]:
        busiest = max(len(rows) for rows in self.orders_by_month.values())
        return [m for m, rows in self.orders_by_month.items() if len(rows) >= FULL_MONTH_SHARE * busiest]

    def template_month(self, month: str) -> str:
        """The latest full dump month with the calendar month of `month`.

        A dump shorter than a year (test dumps) may have none: then its latest full month.
        """
        full = self.full_months
        same_month = [m for m in full if m[5:] == month[5:]]
        return max(same_month or full)

    def growth(self, template: str) -> float:
        """Volume factor of a template from an earlier year than the dump's latest full month.

        Olist grew ~1.5x a year: a September drawn from 2017 at its own size would drop
        a third below the August before it, drawn from 2018. So the template is scaled
        by the latest full month over the same calendar month in the template's year
        (2018-08 / 2017-08); 1 within the latest year or without that month.
        """
        full = self.full_months
        latest = max(full)
        base = f"{template[:4]}-{latest[5:]}"
        if base not in full:
            return 1.0
        return len(self.orders_by_month[latest]) / len(self.orders_by_month[base])


def generate_month(dump: Dump, month: str) -> dict[str, pa.Table]:
    """One generated month: new rows for every order table, customers and sellers."""
    if month <= dump.last_month:
        raise ValueError(f"{month} is inside the dump (until {dump.last_month})")
    rng = random.Random(f"olist-synthetic:{month}")
    template = dump.template_month(month)
    days = (month_start(month) - month_start(template)).days
    # A template day the new month doesn't have (31st into a 30-day month): not drawn
    month_end = month_start(synthetic_next_month(month))
    candidates = [
        row
        for row in dump.orders_by_month[template]
        if month_start(month) + timedelta(days=int(dump.purchased_at[row][8:10]) - 1) < month_end
    ]
    count = round(len(candidates) * dump.growth(template) * rng.uniform(1 - VOLUME_JITTER, 1 + VOLUME_JITTER))
    picks = [rng.choice(candidates) for _ in range(count)]

    real_orders = dump.tables["orders"]
    template_ids = strings(real_orders["order_id"].take(picks))
    template_customers = strings(real_orders["customer_id"].take(picks))
    order_ids = synthetic_ids(month, "order", count)
    customer_ids = synthetic_ids(month, "customer", count)
    person_ids = synthetic_ids(month, "person", count)

    # Customers: the template customer's row, or a person of the dump (repeat order)
    customers = dump.tables["customers"]
    customer_rows, people = [], []
    for i, template_customer in enumerate(template_customers):
        if rng.random() < dump.repeat_share:
            row = rng.randrange(customers.num_rows)
            customer_rows.append(row)
            people.append(customers["customer_unique_id"][row].as_py())
        else:
            customer_rows.append(dump.customer_row[template_customer])
            people.append(person_ids[i])
    new_customers = customers.take(customer_rows)
    new_customers = replace_column(new_customers, "customer_id", customer_ids)
    new_customers = replace_column(new_customers, "customer_unique_id", people)

    orders = real_orders.take(picks)
    orders = replace_column(orders, "order_id", order_ids)
    orders = replace_column(orders, "customer_id", customer_ids)
    generated = {"orders": orders, "customers": new_customers}

    for name in ORDER_CHILDREN:
        rows, owners = [], []
        for i, template_id in enumerate(template_ids):
            for row in dump.children[name].get(template_id, []):
                rows.append(row)
                owners.append(order_ids[i])
        table = dump.tables[name].take(rows)
        generated[name] = replace_column(table, "order_id", owners)

    for name, columns in TIMESTAMPS.items():
        for column in columns:
            generated[name] = replace_column(generated[name], column, shift_days(generated[name][column], days))

    # New sellers: each takes over every item of one template seller in this month
    items = generated["order_items"]
    month_sellers = sorted(set(strings(items["seller_id"])))
    replaced = rng.sample(month_sellers, min(len(month_sellers), max(1, round(NEW_SELLER_SHARE * len(month_sellers)))))
    new_ids = dict(zip(replaced, synthetic_ids(month, "seller", len(replaced)), strict=True))
    generated["order_items"] = replace_column(
        items, "seller_id", [new_ids.get(seller, seller) for seller in strings(items["seller_id"])]
    )
    sellers = dump.tables["sellers"].take([dump.seller_row[old] for old in replaced])
    generated["sellers"] = replace_column(sellers, "seller_id", [new_ids[old] for old in replaced])
    return generated


def generate(dump: Dump, until: str) -> dict[str, pa.Table]:
    """Every generated month after the dump up to `until`, as one table per name."""
    months = months_between(dump.last_month, until)
    parts = [generate_month(dump, month) for month in months]
    return {
        name: pa.concat_tables([part[name] for part in parts]) if parts else dump.tables[name].slice(0, 0)
        for name in TABLES
    }

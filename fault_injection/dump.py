"""A small, consistent Olist dump: every data test passes on it.

Deterministic (no wall clock, fixed seed). Orders run from FIRST_DAY to LAST_DAY so
that Elementary's anomaly tests have their 14 weeks of training data before the last
month, which is the month a fault-injection scenario corrupts.
"""

import csv
import random
from datetime import date, datetime, time, timedelta
from pathlib import Path

FIRST_DAY = date(2018, 1, 1)
LAST_DAY = date(2018, 6, 30)
LAST_CLEAN_MONTH = "2018-05"
ORDERS_PER_DAY = 5

# Names from the seed product_category_name_translation
CATEGORIES = ["beleza_saude", "esporte_lazer", "informatica_acessorios", "moveis_decoracao"]
# zip prefix, city, state, latitude, longitude
PLACES = [
    ("01037", "sao paulo", "SP", -23.5456, -46.6393),
    ("20040", "rio de janeiro", "RJ", -22.9068, -43.1729),
    ("30130", "belo horizonte", "MG", -19.9191, -43.9386),
    ("80010", "curitiba", "PR", -25.4284, -49.2733),
]
FILES = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}
Rows = dict[str, list[dict[str, str]]]


def stamp(value: datetime) -> str:
    return value.strftime("%Y-%m-%d %H:%M:%S")


def clean_rows() -> Rows:
    rng = random.Random(222)
    rows: Rows = {table: [] for table in FILES}
    for zip_code, city, state, lat, lng in PLACES:
        rows["geolocation"].append(
            {
                "geolocation_zip_code_prefix": zip_code,
                "geolocation_lat": str(lat),
                "geolocation_lng": str(lng),
                "geolocation_city": city,
                "geolocation_state": state,
            }
        )
    for i, category in enumerate(CATEGORIES * 3):
        rows["products"].append(
            {
                "product_id": f"product{i:02}",
                "product_category_name": category,
                "product_name_lenght": "40",
                "product_description_lenght": "300",
                "product_photos_qty": "1",
                "product_weight_g": "500",
                "product_length_cm": "20",
                "product_height_cm": "10",
                "product_width_cm": "15",
            }
        )
    for i, (zip_code, city, state, _, _) in enumerate(PLACES):
        rows["sellers"].append(
            {
                "seller_id": f"seller{i}",
                "seller_zip_code_prefix": zip_code,
                "seller_city": city,
                "seller_state": state,
            }
        )
    n = 0
    day = FIRST_DAY
    while day <= LAST_DAY:
        for slot in range(ORDERS_PER_DAY):
            n += 1
            add_order(rows, rng, n, datetime.combine(day, time(8 + 2 * slot, rng.randrange(60))))
        day += timedelta(days=1)
    return rows


def add_order(rows: Rows, rng: random.Random, n: int, purchased_at: datetime) -> None:
    order_id, customer_id = f"order{n:05}", f"customer{n:05}"
    zip_code, city, state, _, _ = rng.choice(PLACES)
    rows["customers"].append(
        {
            "customer_id": customer_id,
            # Every tenth order comes from a returning person
            "customer_unique_id": f"person{n - 10 if n % 10 == 0 else n:05}",
            "customer_zip_code_prefix": zip_code,
            "customer_city": city,
            "customer_state": state,
        }
    )
    carrier_at = purchased_at + timedelta(days=1)
    delivered_at = purchased_at + timedelta(days=rng.randint(3, 8))
    rows["orders"].append(
        {
            "order_id": order_id,
            "customer_id": customer_id,
            "order_status": "delivered",
            "order_purchase_timestamp": stamp(purchased_at),
            "order_approved_at": stamp(purchased_at + timedelta(minutes=10)),
            "order_delivered_carrier_date": stamp(carrier_at),
            "order_delivered_customer_date": stamp(delivered_at),
            "order_estimated_delivery_date": stamp(datetime.combine(purchased_at.date(), time()) + timedelta(days=10)),
        }
    )
    total = 0.0
    for item in range(1, rng.randint(1, 2) + 1):
        price, freight = float(rng.randint(30, 60)), float(rng.randint(5, 15))
        total += price + freight
        rows["order_items"].append(
            {
                "order_id": order_id,
                "order_item_id": str(item),
                "product_id": f"product{rng.randrange(12):02}",
                "seller_id": f"seller{rng.randrange(len(PLACES))}",
                "shipping_limit_date": stamp(purchased_at + timedelta(days=2)),
                "price": f"{price:.2f}",
                "freight_value": f"{freight:.2f}",
            }
        )
    rows["order_payments"].append(
        {
            "order_id": order_id,
            "payment_sequential": "1",
            "payment_type": rng.choice(["credit_card", "boleto"]),
            "payment_installments": "1",
            "payment_value": f"{total:.2f}",
        }
    )
    answered_at = delivered_at + timedelta(days=1)
    rows["order_reviews"].append(
        {
            "review_id": f"review{n:05}",
            "order_id": order_id,
            "review_score": str(rng.choice([3, 4, 4, 5, 5])),
            "review_comment_title": "",
            "review_comment_message": "",
            "review_creation_date": stamp(datetime.combine(answered_at.date(), time())),
            "review_answer_timestamp": stamp(answered_at),
        }
    )


def write_dump(rows: Rows, directory: Path) -> Path:
    directory.mkdir(parents=True)
    for table, file_name in FILES.items():
        header = list(dict.fromkeys(column for row in rows[table] for column in row))
        with (directory / file_name).open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, header)
            writer.writeheader()
            writer.writerows(rows[table])
    return directory

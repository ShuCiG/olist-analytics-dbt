"""Scale test tooling (#250): `ingestion/scale_dump.py` multiplies a dump, and the loader
reads the order tables block by block (`csv_blocks`, `SourceDump.order_keys/order_rows`)
so that a 100x dump never sits in memory whole. No network, tiny CSVs.
"""

import csv
from pathlib import Path

import pyarrow.compute as pc
import pytest

from ingestion import pipeline, scale_dump

ORDERS = [
    ("o1", "c1", "2017-01-04 21:15:19"),
    ("o2", "c2", "2017-01-15 12:16:38"),
    ("o3", "c1", "2017-02-02 22:07:52"),
]
# One review text with a line break and a comma: a block boundary can fall inside it
REVIEWS = [(f"rv_{o}", o, "5", f"Arrived,\nthanks {o}") for o, _, _ in ORDERS]


def write_csv(path: Path, header: list[str], rows: list[tuple]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)


@pytest.fixture
def dump(tmp_path: Path) -> Path:
    src = tmp_path / "dump"
    src.mkdir()
    write_csv(src / "olist_orders_dataset.csv", ["order_id", "customer_id", "order_purchase_timestamp"], ORDERS)
    write_csv(
        src / "olist_order_items_dataset.csv",
        ["order_id", "order_item_id", "price"],
        [(o, str(i), "10.00") for o, _, _ in ORDERS for i in (1, 2)],
    )
    write_csv(src / "olist_order_payments_dataset.csv", ["order_id", "payment_sequential"], [(o, "1") for o, _, _ in ORDERS])
    write_csv(src / "olist_order_reviews_dataset.csv", ["review_id", "order_id", "review_score", "review_comment_message"], REVIEWS)
    write_csv(
        src / "olist_customers_dataset.csv",
        ["customer_id", "customer_unique_id", "customer_state"],
        [("c1", "u1", "SP"), ("c2", "u2", "RJ")],
    )
    write_csv(src / "olist_sellers_dataset.csv", ["seller_id", "seller_state"], [("s1", "SP")])
    return src


def column(path: Path, name: str) -> list[str]:
    return [str(v) for v in scale_dump.read_strings(path)[name].to_pylist()]


def test_factor_one_is_identity(dump: Path, tmp_path: Path) -> None:
    out = tmp_path / "x1"
    scale_dump.scale_dump(dump, out, 1)
    for source in dump.glob("*.csv"):
        assert scale_dump.read_strings(out / source.name).equals(scale_dump.read_strings(source)), source.name


def test_factor_multiplies_orders_and_children(dump: Path, tmp_path: Path) -> None:
    out = tmp_path / "x3"
    rows = scale_dump.scale_dump(dump, out, 3)
    assert rows["olist_orders_dataset.csv"] == 9
    assert rows["olist_order_items_dataset.csv"] == 18
    assert rows["olist_order_payments_dataset.csv"] == 9
    assert rows["olist_order_reviews_dataset.csv"] == 9
    assert rows["olist_customers_dataset.csv"] == 6

    order_ids = column(out / "olist_orders_dataset.csv", "order_id")
    assert len(set(order_ids)) == 9
    assert set(order_ids[:3]) == {"o1", "o2", "o3"}  # copy 0 is the original
    customers = set(column(out / "olist_customers_dataset.csv", "customer_id"))
    assert len(customers) == 6
    assert set(column(out / "olist_orders_dataset.csv", "customer_id")) == customers
    for child in ("items", "payments", "reviews"):
        assert set(column(out / f"olist_order_{child}_dataset.csv", "order_id")) == set(order_ids), child


def test_copies_keep_everything_but_the_ids(dump: Path, tmp_path: Path) -> None:
    out = tmp_path / "x2"
    scale_dump.scale_dump(dump, out, 2)
    orders = scale_dump.read_strings(out / "olist_orders_dataset.csv")
    assert orders["order_purchase_timestamp"].to_pylist() == [t for _, _, t in ORDERS] * 2
    reviews = scale_dump.read_strings(out / "olist_order_reviews_dataset.csv")
    assert reviews["review_id"].to_pylist() == [r[0] for r in REVIEWS] * 2  # labels join on it
    assert reviews["review_comment_message"].to_pylist() == [r[3] for r in REVIEWS] * 2
    # Shared by all copies: copied as they are
    assert (out / "olist_sellers_dataset.csv").read_bytes() == (dump / "olist_sellers_dataset.csv").read_bytes()


def test_scaling_is_deterministic(dump: Path, tmp_path: Path) -> None:
    scale_dump.scale_dump(dump, tmp_path / "a", 3)
    scale_dump.scale_dump(dump, tmp_path / "b", 3)
    for path in (tmp_path / "a").glob("*.csv"):
        assert path.read_bytes() == (tmp_path / "b" / path.name).read_bytes()


@pytest.mark.parametrize("block_size", [64, 100, 1 << 20])
def test_blocks_add_up_to_the_whole_file(dump: Path, block_size: int) -> None:
    path = dump / "olist_order_reviews_dataset.csv"
    blocks = list(pipeline.csv_blocks(path, block_size=block_size))
    assert pipeline.pa.concat_tables(blocks).equals(pipeline.read_csv_as_strings(path))
    if block_size < 200:
        assert len(blocks) > 1  # the small blocks did split the file (and a review text)


def test_blocks_select_columns(dump: Path) -> None:
    blocks = pipeline.csv_blocks(dump / "olist_orders_dataset.csv", ["order_id"], block_size=64)
    assert pipeline.pa.concat_tables(blocks).column_names == ["order_id"]


def test_order_rows_match_a_whole_file_read(dump: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    real_blocks = pipeline.csv_blocks
    monkeypatch.setattr(pipeline, "csv_blocks", lambda path, columns=None: real_blocks(path, columns, block_size=64))
    source = pipeline.SourceDump(dump)
    keys = source.order_keys
    assert keys["order_id"].to_pylist() == ["o1", "o2", "o3"]
    wanted = keys["order_id"].slice(0, 2)
    for name in ("order_items", "order_payments", "order_reviews"):
        whole = pipeline.read_csv_as_strings(dump / pipeline.SOURCE_FILES[name])
        expected = whole.filter(pc.is_in(whole["order_id"], value_set=wanted))
        assert source.order_rows(name, wanted).equals(expected), name
    orders = source.order_rows("orders", wanted)
    assert orders["order_id"].to_pylist() == ["o1", "o2"]
    assert orders[pipeline.SYNTHETIC_FLAG].to_pylist() == [False, False]

"""Scale the Olist dump N times for the scale test (#250).

    uv run python ingestion/scale_dump.py --factor 10 --out data/scaled_10x

Copy k (1..N-1) of every order gets new ids: `order_id`, `customer_id` and
`customer_unique_id` are md5 of the original and k. Everything else stays as it is, so
the months, the order sizes, prices, delays and review scores are the real ones and
only the volume per month grows. Products, sellers, geolocation and the category
translation are shared by all copies; `review_id` is kept (as in synthetic.py, the
review labels join on it). The loader reads the result with `OLIST_DUMP_DIR=<out>`.
"""

import argparse
import hashlib
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pacsv

PARSE = pacsv.ParseOptions(newlines_in_values=True)
SCALED_FILES = {
    "olist_orders_dataset.csv": ["order_id", "customer_id"],
    "olist_order_items_dataset.csv": ["order_id"],
    "olist_order_payments_dataset.csv": ["order_id"],
    "olist_order_reviews_dataset.csv": ["order_id"],
    "olist_customers_dataset.csv": ["customer_id", "customer_unique_id"],
}


def read_strings(path: Path) -> pa.Table:
    names = pacsv.read_csv(path, parse_options=PARSE).schema.names
    return pacsv.read_csv(
        path,
        parse_options=PARSE,
        convert_options=pacsv.ConvertOptions(column_types={n: pa.string() for n in names}, strings_can_be_null=True),
    )


def rekey(values: pa.ChunkedArray, copy: int) -> pa.Array:
    return pa.array([hashlib.md5(f"scale:{copy}:{v}".encode()).hexdigest() for v in values.to_pylist()], pa.string())


def scale_file(source: Path, target: Path, columns: list[str], factor: int) -> int:
    """Write `factor` copies one after the other: memory holds one copy, not all of them."""
    table = read_strings(source)
    with pacsv.CSVWriter(target, table.schema) as writer:
        writer.write_table(table)
        for copy in range(1, factor):
            part = table
            for column in columns:
                part = part.set_column(part.schema.get_field_index(column), column, rekey(table[column], copy))
            writer.write_table(part)
    return table.num_rows * factor


def scale_dump(source_dir: Path, out_dir: Path, factor: int) -> dict[str, int]:
    out_dir.mkdir(parents=True, exist_ok=True)
    rows = {}
    for path in sorted(source_dir.glob("*.csv")):
        if path.name in SCALED_FILES:
            rows[path.name] = scale_file(path, out_dir / path.name, SCALED_FILES[path.name], factor)
        else:
            shutil.copy(path, out_dir / path.name)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--factor", type=int, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--source", type=Path, help="dump directory (default: the Kaggle download)")
    args = parser.parse_args()
    if args.factor < 1:
        parser.error("--factor must be at least 1")
    source = args.source
    if source is None:
        import kagglehub

        source = Path(kagglehub.dataset_download("olistbr/brazilian-ecommerce"))
    for name, count in scale_dump(source, args.out, args.factor).items():
        print(f"{name}: {count} rows")


if __name__ == "__main__":
    main()

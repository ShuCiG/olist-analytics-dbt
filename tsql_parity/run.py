"""Parity runner: T-SQL procedures on SQL Server vs the PySpark/Delta port.

    docker compose -f tsql_parity/docker-compose.yml up -d --wait   (OLIST_CSV_DIR set)
    uv run python -m tsql_parity.run --csv-dir <folder with the Olist CSVs>

Exit code 1 when the faithful port deviates from the T-SQL output: the CI gate.
Writes tsql_parity/report.md (metric | T-SQL | Spark | difference | verdict).
"""

import argparse
import sys
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any

from tsql_parity import mssql, spark_port
from tsql_parity.compare import Comparison, compare

AS_OF = date(2018, 10, 17)  # the last purchase in the dump; pinned instead of GETDATE()
CENT = Decimal("0.01")
REPORT = Path(__file__).parent / "report.md"


@dataclass(frozen=True)
class Procedure:
    name: str
    table: str
    keys: list[str]
    tolerance: Decimal = Decimal(0)


PROCEDURES = [
    Procedure("usp_payment_installments", "out_payment_installments", ["order_id", "payment_sequential"]),
    Procedure("usp_delivery_metrics", "out_delivery_metrics", ["order_id"]),
    Procedure("usp_orders_without_reviews", "out_orders_without_reviews", ["order_id"]),
    Procedure("usp_payment_type_summary", "out_payment_type_summary", ["payment_type"]),
]


def run_tsql() -> dict[str, list[dict[str, Any]]]:
    mssql.execute("exec dbo.usp_payment_installments")
    mssql.execute(f"exec dbo.usp_delivery_metrics @as_of = '{AS_OF.isoformat()}'")
    mssql.execute("exec dbo.usp_orders_without_reviews")
    mssql.execute("exec dbo.usp_payment_type_summary")
    return {p.name: mssql.table_rows(p.table) for p in PROCEDURES}


def run_spark(spark: Any, naive: bool) -> dict[str, list[dict[str, Any]]]:
    frames = {
        "usp_payment_installments": spark_port.payment_installments(spark, naive),
        "usp_delivery_metrics": spark_port.delivery_metrics(spark, AS_OF, naive),
        "usp_orders_without_reviews": spark_port.orders_without_reviews(spark, naive),
        "usp_payment_type_summary": spark_port.payment_type_summary(spark),
    }
    return {name: spark_port.collect(df) for name, df in frames.items()}


def compare_all(left: dict[str, list[dict[str, Any]]], right: dict[str, list[dict[str, Any]]]) -> list[Comparison]:
    return [compare(p.name, left[p.name], right[p.name], p.keys, p.tolerance) for p in PROCEDURES]


def verdict(c: Comparison) -> str:
    return "match" if c.ok else "DIFFERENCE"


def table(comparisons: list[Comparison]) -> list[str]:
    lines = [
        "| metric | T-SQL | Spark | difference | verdict |",
        "|---|---:|---:|---|---|",
    ]
    for c in comparisons:
        diff = f"left only {c.left_only}, right only {c.right_only}, values differ {c.differ}"
        lines.append(
            f"| {c.name}: rows | {c.left_rows:,} | {c.right_rows:,} | {c.row_deviation_pct:.3f} % | {verdict(c)} |"
        )
        lines.append(f"| {c.name}: row buckets | | | {diff} | {verdict(c)} |")
        for col, n in sorted(c.column_diffs.items()):
            lines.append(f"| {c.name}.{col} | | | {n:,} rows differ | DIFFERENCE |")
    return lines


def write_report(faithful: list[Comparison], naive: list[Comparison]) -> str:
    out = [
        "# T-SQL to Spark parity report",
        "",
        f"Pinned date `@as_of` = {AS_OF.isoformat()} (the T-SQL originals used `GETDATE()`).",
        "",
        "## Faithful port (the CI gate)",
        "",
        *table(faithful),
        "",
        "## Naive port (syntax translated only)",
        "",
        "The port a reader writes first. Each deviation below is a semantic difference, not a bug:",
        "see `docs/tsql-spark-differences.md`.",
        "",
        *table(naive),
        "",
    ]
    for c in naive:
        for e in c.examples:
            out.append(f"- `{c.name}` example: {e}")
    text = "\n".join(out) + "\n"
    REPORT.write_text(text, encoding="utf-8", newline="\n")
    return text


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--csv-dir", type=Path, required=True)
    parser.add_argument("--skip-load", action="store_true", help="SQL Server tables already loaded")
    args = parser.parse_args(argv)

    if not args.skip_load:
        mssql.load_raw(args.csv_dir)
    tsql = run_tsql()

    spark = spark_port.session(spark_port.default_warehouse())
    spark_port.load_raw(spark, args.csv_dir)
    faithful = compare_all(tsql, run_spark(spark, naive=False))
    naive = compare_all(tsql, run_spark(spark, naive=True))
    print(write_report(faithful, naive))
    return 0 if all(c.ok for c in faithful) else 1


if __name__ == "__main__":
    sys.exit(main())

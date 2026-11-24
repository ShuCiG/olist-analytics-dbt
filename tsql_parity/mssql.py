"""SQL Server side of the parity harness: run .sql scripts, call procedures, fetch tables."""

import csv
import os
import re
from pathlib import Path
from typing import Any

import pymssql  # pyright: ignore[reportMissingTypeStubs]

SQL_DIR = Path(__file__).parent / "sql"
Row = tuple[Any, ...]


def connect(database: str = "master") -> Any:
    return pymssql.connect(  # pyright: ignore[reportUnknownMemberType]
        server=os.environ.get("MSSQL_HOST", "localhost"),
        port=os.environ.get("MSSQL_PORT", "14330"),
        user="sa",
        password=os.environ.get("MSSQL_SA_PASSWORD", "Parity_Pass1!"),
        database=database,
        autocommit=True,
    )


def run_script(path: Path) -> None:
    """Run a script with `GO` batch separators (sqlcmd syntax, split here)."""
    batches = re.split(r"^GO\s*$", path.read_text(encoding="utf-8"), flags=re.MULTILINE)
    conn = connect()
    try:
        cur = conn.cursor()
        for batch in batches:
            if batch.strip():
                cur.execute(batch)
    finally:
        conn.close()


def query(sql: str, params: tuple[Any, ...] = (), database: str = "parity") -> list[Row]:
    conn = connect(database)
    try:
        cur = conn.cursor()
        cur.execute(sql, params)
        return list(cur.fetchall())
    finally:
        conn.close()


def execute(sql: str, database: str = "parity") -> None:
    conn = connect(database)
    try:
        conn.cursor().execute(sql)
    finally:
        conn.close()


def table_rows(table: str) -> list[dict[str, Any]]:
    """A whole out_* table as dicts keyed by column name."""
    conn = connect("parity")
    try:
        cur = conn.cursor(as_dict=True)
        cur.execute(f"select * from {table}")
        return [dict(r) for r in cur.fetchall()]
    finally:
        conn.close()


def load_reviews(csv_dir: Path) -> None:
    """Reviews have newlines inside quoted comments; BULK INSERT on Linux cannot read them.

    Multi-row INSERT ... VALUES (1000 rows per statement): pymssql executemany sends row by row.
    """
    conn = connect("parity")
    row_sql = "(" + ",".join(["%s"] * 7) + ")"
    try:
        cur = conn.cursor()
        with (csv_dir / "olist_order_reviews_dataset.csv").open(encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            next(reader)
            batch: list[str | None] = []
            for row in reader:
                batch.extend(v if v != "" else None for v in row)
                if len(batch) == 7000:
                    cur.execute("insert into raw_order_reviews values " + ",".join([row_sql] * 1000), tuple(batch))
                    batch = []
            if batch:
                cur.execute(
                    "insert into raw_order_reviews values " + ",".join([row_sql] * (len(batch) // 7)),
                    tuple(batch),
                )
    finally:
        conn.close()


def load_raw(csv_dir: Path) -> None:
    run_script(SQL_DIR / "00_raw.sql")
    run_script(SQL_DIR / "01_bulk_load.sql")
    load_reviews(csv_dir)
    run_script(SQL_DIR / "10_procedures.sql")

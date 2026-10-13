"""dbt's end-of-run summary, explained for the alert emails (Airflow and CI).

dbt ends `build` with a block like:

    Completed with 1 error, 0 partial successes, and 3 warnings:
    [ERROR]: in model stg_olist__sellers (models/...)
    Runtime Error in model stg_olist__sellers (...)
      Binder Error: ...
    [WARNING]: in test relationships_stg_olist__customers_zip_... (models/...)
    [WARNING]: Got 278 results, configured to warn if != 0
    Done. PASS=111 WARN=3 ERROR=1 SKIP=9 NO-OP=0 REUSED=0 TOTAL=124

Test findings get the test's business `description` and `meta.alert_unit` from the
manifest ("278 customers — Customers whose zip code is missing ..."); everything
else (model errors) stays as dbt wrote it. Stdlib only: imported by the Airflow DAG
folder (Airflow's Python) and by .github/scripts (runner's Python).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from html import escape
from pathlib import Path

from . import email_html

DBT_TIME = re.compile(r"^\d{2}:\d{2}:\d{2}(\s+|$)")  # dbt's prefix, alone on blank lines
ANSI = re.compile(r"\x1b\[[0-9;]*m")
SUMMARY_START = re.compile(r"Completed (with|successfully)")
TEST_HEADER = re.compile(r"^\[(ERROR|WARNING)\]: in test (\S+)")
RESULT_COUNT = re.compile(r"Got (\d+) results?")
MAX_LINES = 40


@dataclass
class TestDoc:
    description: str
    unit: str


@dataclass
class Finding:
    level: str  # ERROR | WARNING
    test: str
    count: int | None
    doc: TestDoc | None


def clean(line: str) -> str:
    return DBT_TIME.sub("", ANSI.sub("", line)).rstrip()


def summary(lines: list[str]) -> tuple[str, list[str]]:
    """(`Done. PASS=...` totals line, the block before it) from dbt's output lines.

    Empty when the output has no end-of-run block (dbt never got that far).
    """
    cleaned = [clean(line) for line in lines]
    start = max((i for i, line in enumerate(cleaned) if SUMMARY_START.search(line)), default=None)
    if start is None:
        return "", []
    block: list[str] = []
    for text in cleaned[start:]:
        if text.startswith("Done. PASS="):
            return text, block
        if text.strip() and "compiled code at" not in text:
            block.append(text)
    return "", block


def test_docs(manifest_path: str | Path) -> dict[str, TestDoc]:
    """Test name → business description + unit, for tests that have a description."""
    path = Path(manifest_path)
    if not path.is_file():
        return {}
    nodes = json.loads(path.read_text(encoding="utf-8"))["nodes"].values()
    return {
        n["name"]: TestDoc(n["description"], n["config"].get("meta", {}).get("alert_unit", "rows"))
        for n in nodes
        if n["resource_type"] == "test" and n.get("description")
    }


def findings(block: list[str], docs: dict[str, TestDoc]) -> tuple[list[Finding], list[str]]:
    """Split the block into test findings and the remaining lines (model errors etc.)."""
    found: list[Finding] = []
    rest: list[str] = []
    for line in block[1:]:  # [0] is "Completed with ..."
        header = TEST_HEADER.match(line)
        if header:
            found.append(Finding(header[1], header[2], None, docs.get(header[2])))
            continue
        count = RESULT_COUNT.search(line)
        if count and found and found[-1].count is None:
            found[-1].count = int(count[1])
            continue
        if found and line.startswith(("Failure in test", "  See test failures")):
            continue  # dbt's repeat of the test name / audit hint
        rest.append(line)
    return found, rest


def to_html(done: str, block: list[str], docs: dict[str, TestDoc]) -> str:
    """Errors first (dbt's own words), then data-test findings in business words."""
    found, rest = findings(block, docs)
    parts: list[str] = []
    if rest:
        parts.append(email_html.heading("dbt errors"))
        parts.append(email_html.pre(chr(10).join(rest[:MAX_LINES]), "FAILED"))
    for level, title, status in (
        ("ERROR", "Failed data tests", "FAILED"),
        ("WARNING", "Data quality warnings (nothing failed; the data has these issues)", "STALE"),
    ):
        items = [
            (
                f"{f.count:,} {f.doc.unit if f.doc else 'rows'}" if f.count is not None else "?",
                f.doc.description if f.doc else "(no description in the test's yml)",
                f"dbt test: {f.test}",
            )
            for f in found
            if f.level == level
        ]
        if items:
            parts.append(email_html.heading(title))
            parts.append(email_html.findings(items, status))
    if done:
        parts.append(
            f'<p style="margin:12px 0 0;font:12px {email_html.MONO};color:{email_html.MUTED}">'
            f"{escape(done)}</p>"
        )
    return "".join(parts)

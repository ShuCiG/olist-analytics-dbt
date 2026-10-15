"""alerting/dbt_summary.py: dbt's end-of-run block → failed nodes + test findings
in business words (Airflow and CI alert emails)."""

import json
from pathlib import Path

import pytest

from alerting import dbt_summary

# A `dbt build` log tail as dbt prints it: time prefix, colors, blank spacer lines
FAILED_BUILD = [
    "12:01:05  1 of 124 START sql view model staging.stg_olist__sellers ....... [RUN]",
    "12:01:09  \x1b[31mFailure in test assert_payments_match_order_amount\x1b[0m",
    "12:01:10  ",
    "12:01:10  Completed with 2 errors, 0 partial successes, and 1 warning:",
    "12:01:10  ",
    "12:01:10  [ERROR]: in model stg_olist__sellers (models/staging/stg_olist__sellers.sql)",
    "12:01:10  Runtime Error in model stg_olist__sellers (models/staging/stg_olist__sellers.sql)",
    '12:01:10    Binder Error: Referenced column "seller_town" not found',
    "12:01:10  ",
    "12:01:10  [ERROR]: in test assert_payments_match_order_amount (tests/assert_payments.sql)",
    "12:01:10  Failure in test assert_payments_match_order_amount (tests/assert_payments.sql)",
    "12:01:10    Got 249 results, configured to fail if != 0",
    "12:01:10    compiled code at target/compiled/olist/tests/assert_payments.sql",
    "12:01:10    See test failures:",
    "12:01:10  [WARNING]: in test relationships_customers_zip (models/staging/_stg.yml)",
    "12:01:10  [WARNING]: Got 278 results, configured to warn if != 0",
    "12:01:10  ",
    "12:01:10  Done. PASS=111 WARN=1 ERROR=2 SKIP=9 NO-OP=0 REUSED=0 TOTAL=123",
]


@pytest.fixture
def manifest(tmp_path: Path) -> Path:
    def test_node(name: str, description: str, meta: dict) -> dict:
        return {"name": name, "resource_type": "test", "description": description, "config": {"meta": meta}}

    nodes = {
        "test.a": test_node(
            "relationships_customers_zip", "Customers whose zip code is missing.", {"alert_unit": "customers"}
        ),
        "test.b": test_node("assert_payments_match_order_amount", "Paid <> items + freight.", {}),
        "test.c": test_node("not_null_orders_order_id", "", {}),  # no description: not listed
        "model.d": {"name": "fct_orders", "resource_type": "model", "description": "Orders."},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"nodes": nodes}), encoding="utf-8")
    return path


def test_clean_strips_time_prefix_and_colors() -> None:
    assert dbt_summary.clean("12:01:09  \x1b[31mFailure\x1b[0m  ") == "Failure"
    assert dbt_summary.clean("12:01:10") == ""


def test_summary_finds_totals_and_block() -> None:
    done, block = dbt_summary.summary(FAILED_BUILD)
    assert done == "Done. PASS=111 WARN=1 ERROR=2 SKIP=9 NO-OP=0 REUSED=0 TOTAL=123"
    assert block[0] == "Completed with 2 errors, 0 partial successes, and 1 warning:"
    assert "" not in block
    assert not any("compiled code at" in line for line in block)
    # Lines before the end-of-run block (the run itself) are not part of it
    assert not any("START sql view model" in line for line in block)


def test_summary_of_successful_build() -> None:
    lines = ["12:00:00  Completed successfully", "12:00:00  ", "12:00:00  Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5"]
    assert dbt_summary.summary(lines) == ("Done. PASS=5 WARN=0 ERROR=0 SKIP=0 TOTAL=5", ["Completed successfully"])


def test_summary_without_end_of_run_block() -> None:
    # dbt died before the summary (e.g. a parse error)
    assert dbt_summary.summary(["Encountered an error:", "Parsing Error"]) == ("", [])


def test_findings_split_tests_from_model_errors(manifest: Path) -> None:
    _, block = dbt_summary.summary(FAILED_BUILD)
    docs = dbt_summary.test_docs(manifest)
    found, rest = dbt_summary.findings(block, docs)

    assert [(f.level, f.test, f.count) for f in found] == [
        ("ERROR", "assert_payments_match_order_amount", 249),
        ("WARNING", "relationships_customers_zip", 278),
    ]
    assert found[1].doc == dbt_summary.TestDoc("Customers whose zip code is missing.", "customers")
    # The model error stays as dbt wrote it; the test's repeat lines are dropped
    assert rest[0].startswith("[ERROR]: in model stg_olist__sellers")
    assert any("seller_town" in line for line in rest)
    assert not any(line.startswith(("Failure in test", "  See test failures")) for line in rest)


def test_test_docs(manifest: Path, tmp_path: Path) -> None:
    docs = dbt_summary.test_docs(manifest)
    assert set(docs) == {"relationships_customers_zip", "assert_payments_match_order_amount"}
    assert docs["assert_payments_match_order_amount"].unit == "rows"  # default unit
    assert dbt_summary.test_docs(tmp_path / "missing.json") == {}  # optional in CI


def test_to_html_in_business_words(manifest: Path) -> None:
    done, block = dbt_summary.summary(FAILED_BUILD)
    html = dbt_summary.to_html(done, block, dbt_summary.test_docs(manifest))
    assert "dbt errors" in html and "seller_town" in html
    assert "278 customers" in html and "Customers whose zip code is missing." in html
    assert "249 rows" in html and "Paid &lt;&gt; items + freight." in html  # escaped
    assert html.index("Failed data tests") < html.index("Data quality warnings")
    assert "PASS=111" in html


def test_to_html_without_docs_names_the_test() -> None:
    _, block = dbt_summary.summary(FAILED_BUILD)
    html = dbt_summary.to_html("", block, {})
    assert "(no description in the test&#x27;s yml)" in html
    assert "dbt test: relationships_customers_zip" in html

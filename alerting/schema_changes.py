"""SCHEMA email: a load added columns to raw tables (D61). Shared by the Airflow DAGs
(airflow/dags/olist_alerts.py) and CD (.github/scripts/notify_schema.py).

`new_columns` is the feed status field `ingestion/pipeline.py` prints: table -> the
columns this load added. dlt stores them in raw (as strings, like every source
column); staging lists its columns, so marts and the dashboard don't change.
"""

from __future__ import annotations

from html import escape

from . import email_html


def email(
    new_columns: dict[str, list[str]],
    prefix: str,
    where: str,
    links: list[tuple[str, str]],
    footer: str,
) -> tuple[str, str] | None:
    """(subject, html), or None when the load added no columns. `prefix` starts the
    subject (e.g. "[olist_elt_duckdb]"); `where` names the raw tables' location."""
    if not new_columns:
        return None
    count = sum(len(columns) for columns in new_columns.values())
    tables = ", ".join(sorted(new_columns))
    plural = "s" if count > 1 else ""
    subject = f"{prefix} SCHEMA: {count} new source column{plural} in {tables}"
    items = [
        (column, f"New column in source table {table}", f"raw.{table}.{column}")
        for table, columns in sorted(new_columns.items())
        for column in columns
    ]
    intro = (
        f"The latest load into <b>{escape(where)}</b> found column{plural} the source did not "
        "have before. They are stored in the raw layer as text; staging, marts and the "
        "dashboard are unchanged (staging selects its columns by name)."
    )
    steps = (
        email_html.heading("Next steps")
        + email_html.paragraph(
            "1. Ask the source owner what the column means and whether it stays. "
            "2. Add it to the input contract (data-product/contracts/olist-raw.odcs.yaml). "
            "3. If the business needs it, add it to the staging model and its .yml in a PR."
        )
    )
    html = email_html.page(
        "SCHEMA",
        f"{count} new source column{plural} in {tables}",
        intro,
        email_html.findings(items, "SCHEMA") + steps,
        links=links,
        footer=footer,
    )
    return subject, html

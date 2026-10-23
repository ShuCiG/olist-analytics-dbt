"""SCHEMA email: a load added columns to raw tables or found raw columns missing from
the source (D61). Shared by the Airflow DAGs (airflow/dags/olist_alerts.py) and CD
(.github/scripts/notify_schema.py).

`new_columns` and `removed_columns` are the feed status fields `ingestion/pipeline.py`
prints: table -> column names. New columns are stored in raw (as strings, like every
source column); staging lists its columns, so marts and the dashboard don't change.
A removed column stays in raw and gets nulls, which staging and marts read on.
"""

from __future__ import annotations

from html import escape

from . import email_html


def email(
    new_columns: dict[str, list[str]],
    removed_columns: dict[str, list[str]],
    prefix: str,
    where: str,
    links: list[tuple[str, str]],
    footer: str,
) -> tuple[str, str] | None:
    """(subject, html), or None when the load changed no columns. `prefix` starts the
    subject (e.g. "[olist_elt_duckdb]"); `where` names the raw tables' location."""
    if not new_columns and not removed_columns:
        return None
    added = sum(len(columns) for columns in new_columns.values())
    removed = sum(len(columns) for columns in removed_columns.values())
    tables = ", ".join(sorted({*new_columns, *removed_columns}))
    parts = []
    if added:
        parts.append(f"{added} new source column{'s' if added > 1 else ''}")
    if removed:
        parts.append(f"{removed} source column{'s' if removed > 1 else ''} removed")
    title = f"{' and '.join(parts)} in {tables}"
    items = [
        (column, f"New column in source table {table}", f"raw.{table}.{column}")
        for table, columns in sorted(new_columns.items())
        for column in columns
    ] + [
        (
            column,
            f"Missing from source table {table}: loaded as nulls, staging and marts read them",
            f"raw.{table}.{column}",
        )
        for table, columns in sorted(removed_columns.items())
        for column in columns
    ]
    intro = ""
    body = email_html.findings(items, "SCHEMA")
    if added:
        intro += (
            f"The latest load into <b>{escape(where)}</b> found column{'s' if added > 1 else ''} "
            "the source did not have before. They are stored in the raw layer as text; staging, "
            "marts and the dashboard are unchanged (staging selects its columns by name). "
        )
        body += email_html.heading("Next steps: new columns") + email_html.paragraph(
            "1. Ask the source owner what the column means and whether it stays. "
            "2. Add it to the input contract (data-product/contracts/olist-raw.odcs.yaml). "
            "3. If the business needs it, add it to the staging model and its .yml in a PR."
        )
    if removed:
        intro += (
            f"The source files loaded into <b>{escape(where)}</b> lack column"
            f"{'s' if removed > 1 else ''} the raw layer has. The column stays in raw and the "
            "loaded rows get nulls, so staging, marts and the dashboard show nulls for it; "
            "the build stays green unless a NOT NULL test covers it. Each load reports it "
            "until fixed."
        )
        body += email_html.heading("Next steps: removed columns") + email_html.paragraph(
            "1. Ask the source owner whether the column was dropped on purpose. "
            "2. By mistake: the source restores it; rows loaded meanwhile keep their nulls "
            "until reloaded (pipeline.py --full-refresh). "
            "3. On purpose: remove it from staging and its .yml, and from the input contract "
            "(data-product/contracts/olist-raw.odcs.yaml), in a PR; then reload with "
            "--full-refresh so raw drops the column."
        )
    html = email_html.page(
        "SCHEMA",
        title,
        intro,
        body,
        links=links,
        footer=footer,
    )
    return f"{prefix} SCHEMA: {title}", html

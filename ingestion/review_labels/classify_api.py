"""Label review texts that have no label yet with the Claude API (Message Batches, Claude Haiku 5.5).

The backfill was labelled by Claude Code sessions (labels/*.csv, D92). This is the path for reviews that arrive
later, run by the ingest (`ingestion/pipeline.py`, every Airflow run and deploy) before it loads the labels:
a new review that repeats a labelled text gets that label copied, free; only texts never seen go to the API,
and only with ANTHROPIC_API_KEY set. Nothing open means no API call and no key needed, so on the static
Olist dump it costs nothing. The labels land as a new batch file like any other (labeled_by
`claude-api-haiku-5-5`), which the ingest then loads into raw.review_labels; commit it to keep it.

    uv run python ingestion/review_labels/classify_api.py --dry-run         # what it would send, and the cost
    uv run python ingestion/review_labels/classify_api.py --max-texts 500   # needs ANTHROPIC_API_KEY

Same guidelines as the sessions (guidelines.md is the system prompt); structured output guarantees valid
values. Batches cost half the standard price and usually finish within an hour.
"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any

from labels import HERE, SENTIMENTS, TOPICS, by_text, known_text_labels, read_labels, validate, write_labels

MODEL = "claude-haiku-5-5"
LABELED_BY = "claude-api-haiku-5-5"
# Claude Haiku 5.5 list prices per million tokens; batches are billed at half
PRICE_IN, PRICE_OUT, BATCH_DISCOUNT = 0.10, 0.50, 0.5
# Rough per-text sizes for the estimate: guidelines + text in, label + translation (+ low-effort thinking) out
TOKENS_IN_FIXED, TOKENS_OUT = 1_100, 250
CHARS_PER_TOKEN = 3.5

LABEL_SCHEMA = {
    "type": "object",
    "properties": {
        "sentiment": {"type": "string", "enum": sorted(SENTIMENTS)},
        "topic": {"type": "string", "enum": sorted(TOPICS)},
        "translation_en": {"type": "string"},
    },
    "required": ["sentiment", "topic", "translation_en"],
    "additionalProperties": False,
}


def reviews_from_duckdb(path: str, schema: str = "raw") -> list[dict[str, str]]:
    import duckdb

    with duckdb.connect(path, read_only=True) as con:
        rows = con.sql(
            f"""
            select distinct review_id, review_score,
                coalesce(review_comment_title, ''), coalesce(review_comment_message, '')
            from {schema}.order_reviews
            where review_comment_title is not null or review_comment_message is not null
            """
        ).fetchall()
    keys = ["review_id", "review_score", "review_comment_title", "review_comment_message"]
    return [dict(zip(keys, map(str, row), strict=True)) for row in rows]


def open_texts(groups: dict[str, list[dict[str, str]]], labelled: set[str]) -> list[str]:
    """Text keys none of whose review ids has a label, in a stable order."""
    return sorted(k for k, rows in groups.items() if not any(r["review_id"] in labelled for r in rows))


def estimate_usd(groups: dict[str, list[dict[str, str]]], keys: list[str]) -> float:
    chars = sum(len(groups[k][0]["review_comment_title"]) + len(groups[k][0]["review_comment_message"]) for k in keys)
    tokens_in = len(keys) * TOKENS_IN_FIXED + chars / CHARS_PER_TOKEN
    tokens_out = len(keys) * TOKENS_OUT
    return (tokens_in * PRICE_IN + tokens_out * PRICE_OUT) / 1e6 * BATCH_DISCOUNT


def request(key: str, review: dict[str, str], guidelines: str) -> dict[str, Any]:
    text = {
        "score": int(review["review_score"]),
        "title": review["review_comment_title"] or None,
        "message": review["review_comment_message"] or None,
    }
    return {
        "custom_id": key,
        "params": {
            "model": MODEL,
            # Haiku 5.5 thinks by default; thinking counts toward max_tokens
            "max_tokens": 1024,
            "output_config": {"effort": "low", "format": {"type": "json_schema", "schema": LABEL_SCHEMA}},
            "system": guidelines + "\n\nLabel the review the user sends. Answer with the JSON object only.",
            "messages": [{"role": "user", "content": json.dumps(text, ensure_ascii=False)}],
        },
    }


def label_with_batch(client: Any, requests: list[dict[str, Any]], poll_seconds: float = 60) -> tuple[list[dict[str, str]], list[str]]:
    """Run one batch to the end; returns the labels by key and the keys that failed."""
    batch = client.messages.batches.create(requests=requests)
    print(f"batch {batch.id}: {len(requests)} requests")
    while batch.processing_status != "ended":
        time.sleep(poll_seconds)
        batch = client.messages.batches.retrieve(batch.id)
        print(f"  {batch.processing_status}: {batch.request_counts.processing} processing")
    items: list[dict[str, str]] = []
    failed: list[str] = []
    for result in client.messages.batches.results(batch.id):
        if result.result.type != "succeeded":
            failed.append(f"{result.custom_id}: {result.result.type}")
            continue
        message = result.result.message
        text = next((b.text for b in message.content if b.type == "text"), "")
        if message.stop_reason != "end_turn" or not text:
            failed.append(f"{result.custom_id}: stop_reason {message.stop_reason}")
            continue
        items.append({"key": result.custom_id, **json.loads(text)})
    return items, failed


def label_new_reviews(
    reviews: list[dict[str, str]],
    labels_dir: Path | None = None,
    max_texts: int = 500,
    dry_run: bool = False,
    client: Any = None,
) -> list[Path]:
    """Label the reviews that have none; returns the batch files written.

    Repeated texts first (copied, no API), then at most `max_texts` new texts through one Message Batch.
    Without a client and without ANTHROPIC_API_KEY it only reports what is open.
    """
    groups = by_text(reviews)
    labelled = read_labels(labels_dir)
    written: list[Path] = []
    if copies := known_text_labels(groups, labelled):
        print(f"{len(copies)} labelled texts under new review ids: label copied")
        if not dry_run and (path := write_labels(copies, groups, LABELED_BY, labels_dir)):
            written.append(path)
    keys = open_texts(groups, set(labelled))
    if not keys:
        print("Every review text has a label: nothing to send")
        return written
    picked = keys[:max_texts]
    print(f"{len(keys)} texts without a label; this run: {len(picked)}, ~${estimate_usd(groups, picked):.2f}")
    if dry_run:
        return written
    if client is None:
        if not os.environ.get("ANTHROPIC_API_KEY"):
            print("ANTHROPIC_API_KEY not set: texts left unlabelled")
            return written
        import anthropic  # only when there is something to send

        client = anthropic.Anthropic()
    guidelines = (HERE / "guidelines.md").read_text(encoding="utf-8")
    items, failed = label_with_batch(client, [request(k, groups[k][0], guidelines) for k in picked])
    for line in failed:
        print(f"  not labelled: {line}")
    if path := write_labels(items, groups, LABELED_BY, labels_dir):
        written.append(path)
    return written


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--duckdb", default="data/olist.duckdb", help="DuckDB file with raw.order_reviews")
    parser.add_argument("--max-texts", type=int, default=500, help="cost guard: texts per run (default %(default)s)")
    parser.add_argument("--dry-run", action="store_true", help="count and estimate only, no API call")
    args = parser.parse_args()
    if label_new_reviews(reviews_from_duckdb(args.duckdb), max_texts=args.max_texts, dry_run=args.dry_run):
        validate()


if __name__ == "__main__":
    main()

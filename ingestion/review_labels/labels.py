"""Review labels (sentiment, topic, English translation) kept as CSV batches next to the code.

The labels come from outside the warehouse (an LLM following guidelines.md), so dbt reads them as data, not as a model:
every engine gets the same labels and builds stay deterministic. One label per review text: identical texts ("bom",
"recomendo") under different review ids are labelled once and the label is written for each id.

Standard library only, except `export-queue` (DuckDB): the other commands also run in a bare cloud session.

    python ingestion/review_labels/labels.py export-queue          # local: queue/ from data/olist.duckdb
    python ingestion/review_labels/labels.py status
    python ingestion/review_labels/labels.py next --limit 100 --out work/next.jsonl
    python ingestion/review_labels/labels.py write work/labels.jsonl --labeled-by claude-code-routine
    python ingestion/review_labels/labels.py validate
"""

import argparse
import csv
import hashlib
import json
import sys
from collections.abc import Iterable
from datetime import UTC, datetime
from pathlib import Path

HERE = Path(__file__).parent
QUEUE_DIR = HERE / "queue"  # part_NN.csv, each under the 1 MB pre-commit limit
QUEUE_PART_ROWS = 7_000
LABELS_DIR = HERE / "labels"
GUIDELINES_VERSION = 1

SENTIMENTS = {"positive", "negative", "mixed", "neutral"}
TOPICS = {"delivery", "product_quality", "wrong_or_missing_item", "seller_service", "general", "other"}
QUEUE_COLUMNS = ["review_id", "review_score", "review_comment_title", "review_comment_message"]
LABEL_COLUMNS = ["review_id", "sentiment", "topic", "translation_en", "labeled_by", "guidelines_version"]


def text_key(title: str, message: str) -> str:
    return hashlib.md5(f"{title}\x1f{message}".encode()).hexdigest()


def read_queue() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for path in sorted(QUEUE_DIR.glob("part_*.csv")):
        with path.open(encoding="utf-8", newline="") as f:
            rows += csv.DictReader(f)
    return rows


def read_labels(labels_dir: Path | None = None) -> dict[str, dict[str, str]]:
    labels: dict[str, dict[str, str]] = {}
    for path in sorted((labels_dir or LABELS_DIR).glob("*.csv")):
        with path.open(encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                labels[row["review_id"]] = row
    return labels


def by_text(queue: Iterable[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    groups: dict[str, list[dict[str, str]]] = {}
    for row in queue:
        key = text_key(row["review_comment_title"], row["review_comment_message"])
        groups.setdefault(key, []).append(row)
    return groups


def export_queue(duckdb_path: str) -> None:
    import duckdb  # local only

    con = duckdb.connect(duckdb_path, read_only=True)
    rows = con.sql(
        """
        select distinct review_id, review_score, coalesce(review_comment_title, ''), coalesce(review_comment_message, '')
        from raw.order_reviews
        where review_comment_title is not null or review_comment_message is not null
        order by review_id
        """
    ).fetchall()
    QUEUE_DIR.mkdir(exist_ok=True)
    for old in QUEUE_DIR.glob("part_*.csv"):
        old.unlink()
    for part, start in enumerate(range(0, len(rows), QUEUE_PART_ROWS), 1):
        with (QUEUE_DIR / f"part_{part:02d}.csv").open("w", encoding="utf-8", newline="") as f:
            w = csv.writer(f, lineterminator="\n")
            w.writerow(QUEUE_COLUMNS)
            w.writerows(rows[start : start + QUEUE_PART_ROWS])
    print(f"{len(rows)} reviews -> {QUEUE_DIR}")


def status() -> None:
    groups = by_text(read_queue())
    labels = read_labels()
    open_texts = [k for k, rows in groups.items() if not any(r["review_id"] in labels for r in rows)]
    labelled_ids = sum(1 for rows in groups.values() for r in rows if r["review_id"] in labels)
    print(f"reviews: {sum(map(len, groups.values()))}, labelled: {labelled_ids}")
    print(f"distinct texts: {len(groups)}, still open: {len(open_texts)}")
    if copies := known_text_labels(groups, labels):
        print(f"labelled texts with unlabelled review ids: {len(copies)} (no labeller needed, see known_text_labels)")


def next_batch(limit: int, out: Path) -> None:
    groups = by_text(read_queue())
    labels = read_labels()
    # md5 order: a stable mix of scores and lengths, the same for every run
    open_keys = sorted(k for k, rows in groups.items() if not any(r["review_id"] in labels for r in rows))[:limit]
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", encoding="utf-8") as f:
        for key in open_keys:
            row = groups[key][0]
            item = {
                "key": key,
                "score": int(row["review_score"]),
                "title": row["review_comment_title"] or None,
                "message": row["review_comment_message"] or None,
            }
            f.write(json.dumps(item, ensure_ascii=False) + "\n")
    print(f"{len(open_keys)} texts -> {out}")


def write_labels(
    items: list[dict[str, str]],
    groups: dict[str, list[dict[str, str]]],
    labeled_by: str,
    labels_dir: Path | None = None,
) -> Path | None:
    """One new batch file: each label (by text `key`) for every review id with that text and no label yet.

    An item's own `labeled_by` wins over the argument (labels copied from another review keep their labeller).
    Raises ValueError listing every bad item; returns None when nothing is new.
    """
    labels_dir = labels_dir or LABELS_DIR
    existing = read_labels(labels_dir)
    rows: list[list[str | int]] = []
    errors: list[str] = []
    for n, item in enumerate(items, 1):
        key = item.get("key")
        if key not in groups:
            errors.append(f"item {n}: unknown key {key!r}")
            continue
        errors += [f"item {n}: {e}" for e in check_label(item)]
        for review in groups[key]:
            if review["review_id"] not in existing:
                rows.append([review["review_id"], item["sentiment"], item["topic"], item["translation_en"].strip(),
                             item.get("labeled_by") or labeled_by, GUIDELINES_VERSION])
    if errors:
        raise ValueError("\n".join(errors))
    if not rows:
        return None
    stamp = f"batch_{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    path = labels_dir / f"{stamp}.csv"
    n = 1
    while path.exists():  # two batches in one second (copies, then the API's)
        n += 1
        path = labels_dir / f"{stamp}_{n}.csv"
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f, lineterminator="\n")
        w.writerow(LABEL_COLUMNS)
        w.writerows(sorted(rows))
    print(f"{len(rows)} reviews -> {path}")
    return path


def known_text_labels(groups: dict[str, list[dict[str, str]]], labels: dict[str, dict[str, str]]) -> list[dict[str, str]]:
    """Labels for texts already labelled under another review id, as items for `write_labels`.

    A new review that repeats a labelled text ("bom") gets that label for free, with no labeller
    run; the label of the smallest labelled review id is taken, so reruns pick the same one.
    """
    items = []
    for key, rows in sorted(groups.items()):
        ids = sorted(r["review_id"] for r in rows)
        done = [i for i in ids if i in labels]
        if done and len(done) < len(ids):
            src = labels[done[0]]
            items.append({"key": key, **{c: src[c] for c in ("sentiment", "topic", "translation_en", "labeled_by")}})
    return items


def write_batch(labels_jsonl: Path, labeled_by: str) -> None:
    lines = labels_jsonl.read_text(encoding="utf-8").splitlines()
    items = [json.loads(line) for line in lines if line.strip()]
    try:
        path = write_labels(items, by_text(read_queue()), labeled_by)
    except ValueError as e:
        sys.exit(str(e))
    if path is None:
        sys.exit("nothing new to write")
    validate()


def check_label(row: dict[str, str]) -> list[str]:
    errors = []
    if row.get("sentiment") not in SENTIMENTS:
        errors.append(f"sentiment {row.get('sentiment')!r}")
    if row.get("topic") not in TOPICS:
        errors.append(f"topic {row.get('topic')!r}")
    if not (row.get("translation_en") or "").strip():
        errors.append("empty translation_en")
    return errors


def validate() -> None:
    queue_ids = {r["review_id"] for r in read_queue()} if QUEUE_DIR.exists() else None
    seen: dict[str, str] = {}
    errors: list[str] = []
    for path in sorted(LABELS_DIR.glob("*.csv")):
        with path.open(encoding="utf-8", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames != LABEL_COLUMNS:
                errors.append(f"{path.name}: header {reader.fieldnames}")
                continue
            for n, row in enumerate(reader, 2):
                where = f"{path.name}:{n}"
                errors += [f"{where}: {e}" for e in check_label(row)]
                rid = row["review_id"]
                if rid in seen:
                    errors.append(f"{where}: review_id {rid} also in {seen[rid]}")
                seen[rid] = path.name
                if queue_ids is not None and rid not in queue_ids:
                    errors.append(f"{where}: review_id {rid} not in the queue")
    if errors:
        sys.exit("\n".join(errors[:50]) + (f"\n... {len(errors)} errors" if len(errors) > 50 else ""))
    print(f"OK: {len(seen)} labelled reviews in {len(list(LABELS_DIR.glob('*.csv')))} files")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("export-queue")
    p.add_argument("--duckdb", default="data/olist.duckdb")
    sub.add_parser("status")
    p = sub.add_parser("next")
    p.add_argument("--limit", type=int, default=100)
    p.add_argument("--out", type=Path, default=HERE / "work" / "next.jsonl")
    p = sub.add_parser("write")
    p.add_argument("labels", type=Path)
    p.add_argument("--labeled-by", required=True)
    sub.add_parser("validate")
    args = parser.parse_args()
    if args.command == "export-queue":
        export_queue(args.duckdb)
    elif args.command == "status":
        status()
    elif args.command == "next":
        next_batch(args.limit, args.out)
    elif args.command == "write":
        write_batch(args.labels, args.labeled_by)
    else:
        validate()


if __name__ == "__main__":
    main()

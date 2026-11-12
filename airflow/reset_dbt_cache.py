"""Drop the dbt partial parse caches when the dbt project's files change (#279, D114).

dbt-core 1.12 duplicates a v2 metric when a partial parse re-reads the YAML file
that defines it ("dbt found two metrics with the name ..."). The caches on the
dbt-cache volume (D55) outlive the image, so the first run after an upgrade that
edits such a file failed. airflow-init runs this script after every image change:
when the project's files differ from the ones the caches were written for, every
partial_parse.msgpack under the cache dir is deleted, and each cache pays one full
parse on its next call. An image that changes only Python or docs keeps the caches.

Standard library only: init runs it on Airflow's own Python.

    python reset_dbt_cache.py --project /opt/project/transform --cache /opt/dbt-cache
"""

import argparse
import hashlib
from pathlib import Path

STAMP = "project_hash"
CACHE_FILE = "partial_parse.msgpack"
# Generated or installed, not sources: dbt_packages follows package-lock.yml
_SKIP_DIRS = {"dbt_packages", "logs", "edr_target", "__pycache__"}


def _is_source(rel: Path) -> bool:
    for part in rel.parts[:-1]:
        if part in _SKIP_DIRS or part.startswith(("target", ".")):
            return False
    return not rel.name.startswith(".")


def project_hash(project_dir: Path) -> str:
    """sha256 over every source file of the dbt project: relative path + bytes."""
    digest = hashlib.sha256()
    files = sorted(
        (p.relative_to(project_dir) for p in project_dir.rglob("*") if p.is_file()),
        key=lambda rel: rel.as_posix(),
    )
    for rel in files:
        if not _is_source(rel):
            continue
        digest.update(rel.as_posix().encode() + b"\0")
        digest.update((project_dir / rel).read_bytes() + b"\0")
    return digest.hexdigest()


def reset(project_dir: Path, cache_dir: Path) -> list[Path]:
    """Delete the caches if they were written for other project files; return them."""
    current = project_hash(project_dir)
    stamp = cache_dir / STAMP
    if stamp.is_file() and stamp.read_text(encoding="utf-8").strip() == current:
        return []
    removed = sorted(cache_dir.rglob(CACHE_FILE))
    for path in removed:
        path.unlink()
    cache_dir.mkdir(parents=True, exist_ok=True)
    stamp.write_text(current + "\n", encoding="utf-8")
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description="Drop dbt partial parse caches when the dbt project changes")
    parser.add_argument("--project", type=Path, required=True, help="dbt project dir")
    parser.add_argument("--cache", type=Path, required=True, help="dbt cache volume")
    args = parser.parse_args()
    removed = reset(args.project, args.cache)
    if removed:
        print(f"dbt project changed: removed {len(removed)} {CACHE_FILE} under {args.cache}")
        for path in removed:
            print(f"  {path}")
    else:
        print(f"no stale {CACHE_FILE} under {args.cache}: caches kept")


if __name__ == "__main__":
    main()

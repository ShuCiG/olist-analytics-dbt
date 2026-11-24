"""Block a push that adds a raw source table prod `raw` does not have yet (#353).

PR CI reads the sources of unchanged and new models from prod `raw` (slim CI, defer), so a
source table added in a `sources.yml` fails `dbt build (BigQuery)` until someone loads it
there once. The load is the owner's: it writes to prod.

    uv run python .github/scripts/check_new_sources.py             # vs origin/main (pre-push)
    uv run python .github/scripts/check_new_sources.py --base HEAD~3

Exit 1 when a table new against the base is missing in prod `raw`, with the load command.
Without a base to compare with or without Google credentials it warns and exits 0: CI is
the net then.
"""

import argparse
import os
import re
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]
SOURCES_GLOB = "transform/models/**/*sources.yml"
PROD_RAW = "raw"
LOAD_COMMAND = "uv run python ingestion/pipeline.py --destination bigquery"
_ENV_DEFAULT = re.compile(r"env_var\(\s*'[^']+'\s*,\s*'([^']*)'\s*\)")


def raw_tables(text: str) -> set[str]:
    """Names of the tables of the sources whose schema defaults to `raw`."""
    found: set[str] = set()
    for source in (yaml.safe_load(text) or {}).get("sources", []):
        schema = str(source.get("schema", source["name"]))
        match = _ENV_DEFAULT.search(schema)
        if (match.group(1) if match else schema) == PROD_RAW:
            found.update(t["name"] for t in source.get("tables", []))
    return found


def new_tables(before: dict[str, str | None], after: dict[str, str]) -> set[str]:
    """Tables in `after` (path -> yml) that `before` (same paths, None = new file) lacks."""
    new: set[str] = set()
    for path, text in after.items():
        old = before.get(path)
        new |= raw_tables(text) - (raw_tables(old) if old else set())
    return new


def git(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args], cwd=ROOT, capture_output=True, text=True, encoding="utf-8", errors="replace"
    )


def source_files() -> dict[str, str]:
    return {
        p.relative_to(ROOT).as_posix(): p.read_text(encoding="utf-8") for p in sorted(ROOT.glob(SOURCES_GLOB))
    }


def files_at(base: str, paths: list[str]) -> dict[str, str | None]:
    out: dict[str, str | None] = {}
    for path in paths:
        shown = git("show", f"{base}:{path}")
        out[path] = shown.stdout if shown.returncode == 0 else None
    return out


def missing_in_prod(tables: set[str]) -> list[str]:
    """Tables `raw` has not got. Raises `Unreadable` when prod can't be asked."""
    try:
        from google.api_core.exceptions import NotFound
        from google.cloud import bigquery

        project = os.environ.get("GCP_PROJECT_ID", "olist-analytics-dbt-472315")
        client = bigquery.Client(project=project)
        missing = []
        for table in sorted(tables):
            try:
                client.get_table(f"{project}.{PROD_RAW}.{table}")
            except NotFound:
                missing.append(table)
        return missing
    except Exception as e:  # no ADC, no permission, offline: the check can't say
        if type(e).__name__ == "NotFound":
            raise
        raise Unreadable(f"{type(e).__name__}: {str(e).splitlines()[0] if str(e) else ''}") from e


class Unreadable(Exception):
    pass


def check(
    base: str, ask: Callable[[set[str]], list[str]] = missing_in_prod, out=sys.stdout, err=sys.stderr
) -> int:
    if git("rev-parse", "--verify", "--quiet", base).returncode:
        print(f"check_new_sources: no {base} to compare with: skipped (CI still checks)", file=err)
        return 0
    after = source_files()
    new = new_tables(files_at(base, list(after)), after)
    if not new:
        print(f"check_new_sources: no new raw source table against {base}", file=out)
        return 0
    try:
        missing = ask(new)
    except Unreadable as e:
        print(
            f"check_new_sources: WARNING new raw source table(s) {', '.join(sorted(new))}, but prod raw "
            f"can't be read ({e}). Check `gcloud auth application-default login`; CI still checks.",
            file=err,
        )
        return 0
    if not missing:
        print(f"check_new_sources: new raw table(s) {', '.join(sorted(new))} exist in prod raw", file=out)
        return 0
    print(
        f"check_new_sources: prod raw has no {', '.join(f'raw.{t}' for t in missing)}.\n"
        "PR CI (dbt build on BigQuery) reads prod raw and would fail. Load once, with the owner's OK\n"
        f"(it writes to prod):\n    {LOAD_COMMAND}\n"
        "then push again. Skip on purpose with `git push --no-verify`.",
        file=err,
    )
    return 1


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--base", default="origin/main", help="ref to compare the sources.yml files with")
    return check(parser.parse_args(argv).base)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

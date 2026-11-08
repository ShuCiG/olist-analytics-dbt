"""Decide whether a change needs the dbt builds and the prod deploy (job `changes` in ci-cd.yml).

Reads changed paths (one per line, `git diff --name-only`) from stdin and prints
`code=true` or `code=false` for $GITHUB_OUTPUT. `false` only when every path is
documentation that no build reads: Markdown outside transform/ (dbt docs blocks
live in transform/models/*.md and feed dbt docs), the issue forms and the README
screenshots in docs/images/. Anything
else, or no paths at all, counts as code: when in doubt, build.

Also `deletion=true` when a path is code of the deletion requests (D105): the
BigQuery deletion job runs only then (two full builds and a fresh load). The
DuckDB deletion test runs with fault injection on every code change.
"""

import sys
from pathlib import PurePosixPath

DOCS_DIRS = (".github/ISSUE_TEMPLATE/", "docs/images/")
BUILD_DIRS = ("transform/",)
DELETION_PATHS = (
    "transform/macros/deletion_requests.sql",
    "transform/macros/customer_key.sql",
    "ingestion/pipeline.py",
)


def is_docs(path: str) -> bool:
    if path.startswith(DOCS_DIRS):
        return True
    return PurePosixPath(path).suffix.lower() == ".md" and not path.startswith(BUILD_DIRS)


def needs_build(paths: list[str]) -> bool:
    return not paths or not all(is_docs(p) for p in paths)


def touches_deletion(paths: list[str]) -> bool:
    return not paths or any(p in DELETION_PATHS for p in paths)


def main() -> None:
    paths = [line.strip() for line in sys.stdin if line.strip()]
    code = needs_build(paths)
    print(f"code={str(code).lower()}")
    print(f"deletion={str(touches_deletion(paths)).lower()}")
    docs = [p for p in paths if is_docs(p)]
    print(f"{len(paths)} changed, {len(docs)} docs-only, build: {code}", file=sys.stderr)


if __name__ == "__main__":
    main()

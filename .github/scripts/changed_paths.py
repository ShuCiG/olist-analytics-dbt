"""Decide whether a change needs the dbt builds and the prod deploy (job `changes` in ci-cd.yml).

Reads changed paths (one per line, `git diff --name-only`) from stdin and prints
`code=true` or `code=false` for $GITHUB_OUTPUT. `false` only when every path is
documentation that no build reads: Markdown outside transform/ (dbt docs blocks
live in transform/models/*.md and feed dbt docs) and the issue forms. Anything
else, or no paths at all, counts as code: when in doubt, build.
"""

import sys
from pathlib import PurePosixPath

DOCS_DIRS = (".github/ISSUE_TEMPLATE/",)
BUILD_DIRS = ("transform/",)


def is_docs(path: str) -> bool:
    if path.startswith(DOCS_DIRS):
        return True
    return PurePosixPath(path).suffix.lower() == ".md" and not path.startswith(BUILD_DIRS)


def needs_build(paths: list[str]) -> bool:
    return not paths or not all(is_docs(p) for p in paths)


def main() -> None:
    paths = [line.strip() for line in sys.stdin if line.strip()]
    code = needs_build(paths)
    print(f"code={str(code).lower()}")
    docs = [p for p in paths if is_docs(p)]
    print(f"{len(paths)} changed, {len(docs)} docs-only, build: {code}", file=sys.stderr)


if __name__ == "__main__":
    main()

"""Contract changes are versioned: run on a PR against main (CI job `python tests`).

For every data contract that differs from the base branch:
- its version must go up,
- a breaking change (`datacontract breaking`: a removed column, a changed type, a
  dropped promise) needs a new major version.

Consumers pin a major version (data-product/consumers/*.yaml), so a major bump
fails their agreement in CI until they are updated: a breaking change can't reach
a consumer silently.

    uv run python data-product/check_compatibility.py [base-ref]   # default origin/main
"""

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import yaml

REPO = Path(__file__).resolve().parents[1]
CONTRACTS = Path("data-product/contracts")


def parse_version(version: str) -> tuple[int, ...]:
    return tuple(int(part) for part in version.split("."))


def verdict(old_version: str, new_version: str, changed: bool, breaking: bool) -> str | None:
    """None if the change is versioned correctly, else what is wrong."""
    if not changed:
        return None
    old, new = parse_version(old_version), parse_version(new_version)
    if new <= old:
        return f"the contract changed, but its version did not go up ({old_version} -> {new_version})"
    if breaking and new[0] == old[0]:
        return f"breaking change without a new major version ({old_version} -> {new_version})"
    return None


def base_file(base_ref: str, path: Path) -> str | None:
    done = subprocess.run(
        ["git", "show", f"{base_ref}:{path.as_posix()}"], cwd=REPO, capture_output=True, text=True, encoding="utf-8"
    )
    return done.stdout if done.returncode == 0 else None


def is_breaking(old_text: str, new_path: Path) -> bool:
    exe = shutil.which("datacontract", path=str(Path(sys.executable).parent))
    assert exe, f"datacontract not found next to {sys.executable}"
    with tempfile.TemporaryDirectory() as tmp:
        old_path = Path(tmp) / new_path.name
        old_path.write_text(old_text, encoding="utf-8")
        # No inlining: it would fetch the dbt docs pages the contract links to
        done = subprocess.run(
            [exe, "breaking", "--no-inline-references", str(old_path), str(REPO / new_path)],
            capture_output=True, text=True, encoding="utf-8",
        )
    print(done.stdout)
    return done.returncode != 0


def main(base_ref: str = "origin/main") -> int:
    problems = []
    for path in sorted((REPO / CONTRACTS).glob("*.odcs.yaml")):
        rel = path.relative_to(REPO)
        name = rel.as_posix()
        old_text = base_file(base_ref, rel)
        if old_text is None:
            print(f"{name}: new contract")
            continue
        old, new = yaml.safe_load(old_text), yaml.safe_load(path.read_text(encoding="utf-8"))
        changed = old != new  # comments and formatting don't count
        breaking = changed and is_breaking(old_text, rel)
        problem = verdict(old["version"], new["version"], changed, breaking)
        status = problem or ("unchanged" if not changed else f"{old['version']} -> {new['version']}, ok")
        print(f"{name}: {status}")
        if problem:
            problems.append(f"{name}: {problem}")
    for problem in problems:
        print(f"::error file={problem.split(':', 1)[0]}::{problem}")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(*sys.argv[1:]))

"""Helpers for the /audit skill (security review by four agents).

Subcommands:
  check            .audit/ is gitignored and nothing under it is tracked (exit 1 otherwise)
  prepare <dir>    write <dir>/_context.md: tracked files, local-only files, secrets in git history
  redact <dir>     mask secrets in every .md report under <dir>, in place

Nothing here ever prints a secret value: findings are reported as pattern name, file
and commit, redactions as counts.
"""

import json
import os
import re
import subprocess
import sys
from collections.abc import Iterable, Iterator
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]

# (name, regex, group holding the secret; 0 = the whole match)
PATTERNS: list[tuple[str, re.Pattern[str], int]] = [
    (
        "private key block",
        re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----.*?(?:-----END [A-Z ]*PRIVATE KEY-----|$)", re.S),
        0,
    ),
    ("GCP private_key_id", re.compile(r'"private_key_id"\s*:\s*"([0-9a-f]{20,})"'), 1),
    ("Google API key", re.compile(r"AIza[0-9A-Za-z_\-]{35}"), 0),
    ("AWS access key", re.compile(r"AKIA[0-9A-Z]{16}"), 0),
    ("GitHub token", re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})"), 0),
    ("Slack token", re.compile(r"\bxox[abprs]-[A-Za-z0-9-]{10,}"), 0),
    ("credentials in URL", re.compile(r"\b[a-z][a-z0-9+.\-]*://[^\s:/@'\"]+:([^\s@/'\"]+)@"), 1),
    (
        "password / token assignment",
        re.compile(r"(?i)\b(?:password|passwd|secret|token|api_key)\b\s*[:=]\s*['\"]([^'\"\s]{8,})['\"]"),
        1,
    ),
]

SECRET_KEY_NAME = re.compile(r"(?i)pass|secret|token|key|conn|credential|auth")
SKIP_DIRS = {".venv", ".git", "dbt_packages", "node_modules", "data", "target", ".audit"}
MIN_SECRET_LEN = 8
# Build output and caches: noise in the local-only list
LOCAL_NOISE = ("__pycache__/", ".pytest_cache/", "transform/target", "transform/logs/", "transform/dbt_packages/",
               "transform/edr_target/", "transform/.user.yml")


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", *args], cwd=REPO, check=True, capture_output=True, text=True, encoding="utf-8", errors="replace"
    ).stdout


def _walk(names: set[str]) -> Iterator[Path]:
    for root, dirs, files in os.walk(REPO):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for f in files:
            if f in names or any(f.endswith(n) for n in names if n.startswith(".")):
                yield Path(root) / f


def _unquote(value: str) -> str:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
        value = value[1:-1]
    return value


def local_secret_values() -> set[str]:
    """Values of the local secret stores (.env, secrets.toml, GCP key), to mask verbatim."""
    values: set[str] = set()
    for path in _walk({".env", "secrets.toml"}):
        for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
            m = re.match(r"\s*(?:export\s+)?([A-Za-z0-9_.\-]+)\s*[=:]\s*(.+)", line)
            if not m or line.lstrip().startswith("#"):
                continue
            key, value = m.group(1), _unquote(m.group(2))
            if path.name == "secrets.toml" or SECRET_KEY_NAME.search(key):
                values.add(value)
    key_files = [Path(p) for p in [os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")] if p]
    key_files += [p for p in REPO.rglob("*.json") if "service-account" in p.name and ".venv" not in p.parts]
    for path in key_files:
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            continue
        if isinstance(data, dict):
            values.update(str(data[k]) for k in ("private_key", "private_key_id") if data.get(k))
    return {v for v in values if len(v) >= MIN_SECRET_LEN}


def _mask(secret: str) -> str:
    return f"{secret[:4]}…[REDACTED]"


def redact_text(text: str, literals: Iterable[str] = ()) -> tuple[str, int]:
    """Mask known secret values and secret-looking patterns; returns (text, count)."""
    count = 0
    for value in sorted(literals, key=len, reverse=True):
        n = text.count(value)
        if n:
            text, count = text.replace(value, _mask(value)), count + n
    for _, pattern, group in PATTERNS:

        def repl(m: re.Match[str], group: int = group) -> str:
            nonlocal count
            secret = m.group(group)
            if "[REDACTED]" in secret:
                return m.group(0)
            count += 1
            start, end = m.start(group) - m.start(0), m.end(group) - m.start(0)
            return m.group(0)[:start] + _mask(secret) + m.group(0)[end:]

        text = pattern.sub(repl, text)
    return text, count


def scan_history() -> list[tuple[str, str, str]]:
    """(commit, file, pattern name) for every secret-looking line ever added or removed."""
    hits: set[tuple[str, str, str]] = set()
    commit = path = ""
    for line in _git("log", "--all", "-p", "--no-color", "--format=commit %h").splitlines():
        if line.startswith("commit "):
            commit = line.split()[1]
        elif line.startswith("diff --git "):
            path = line.rsplit(" b/", 1)[-1]
        elif line[:1] in "+-" and not line.startswith(("+++", "---")):
            for name, pattern, _ in PATTERNS:
                if pattern.search(line):
                    hits.add((commit, path, name))
    return sorted(hits)


def check() -> int:
    ignored = subprocess.run(["git", "check-ignore", "-q", ".audit/probe.md"], cwd=REPO).returncode == 0
    tracked = _git("ls-files", ".audit").strip()
    if not ignored:
        print("FAIL: .audit/ is not gitignored")
    if tracked:
        print(f"FAIL: files under .audit/ are tracked by git:\n{tracked}")
    if ignored and not tracked:
        print("OK: .audit/ is gitignored and untracked")
    return 0 if ignored and not tracked else 1


def prepare(out_dir: Path) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    tracked = _git("ls-files").splitlines()
    local = [
        line[3:]
        for line in _git("status", "--ignored", "--porcelain", "--untracked-files=normal").splitlines()
        if line.startswith("!! ")
        and not line[3:].startswith(tuple(f"{d}/" for d in SKIP_DIRS))
        and not any(n in line[3:] for n in LOCAL_NOISE)
    ]
    history = scan_history()
    lines = [
        "# Audit context",
        "",
        "Prepared by the dispatcher. File names and commit ids only; no secret values.",
        "",
        f"## Files tracked by git ({len(tracked)}): these are public on GitHub",
        "",
        *[f"- {f}" for f in tracked],
        "",
        "## Local-only (gitignored) files present on this machine",
        "",
        "Never open the secret stores among them (`.env`, `secrets.toml`, key files): judge them by how",
        "the code handles them (gitignore, .dockerignore, mounts, logs).",
        "",
        *([f"- {f}" for f in local] or ["- (none)"]),
        "",
        "## Secret-looking lines in git history (all branches)",
        "",
        "Pattern match on added/removed lines; may include placeholders in docs.",
        "",
        *([f"- {c} `{p}`: {n}" for c, p, n in history] or ["- (none)"]),
        "",
    ]
    (out_dir / "_context.md").write_text("\n".join(lines), encoding="utf-8", newline="\n")
    print(f"context: {len(tracked)} tracked, {len(local)} local-only, {len(history)} history hits")
    return 0


def redact(out_dir: Path) -> int:
    literals = local_secret_values()
    for path in sorted(out_dir.glob("*.md")):
        text, n = redact_text(path.read_text(encoding="utf-8"), literals)
        if n:
            path.write_text(text, encoding="utf-8", newline="\n")
        print(f"{path.name}: {n} redaction(s)")
    return 0


def main(argv: list[str]) -> int:
    if argv[:1] == ["check"]:
        return check()
    if len(argv) == 2 and argv[0] in ("prepare", "redact"):
        out_dir = (REPO / argv[1]).resolve()
        if REPO / ".audit" not in out_dir.parents:
            print("FAIL: output directory must be under .audit/")
            return 1
        return prepare(out_dir) if argv[0] == "prepare" else redact(out_dir)
    print(__doc__)
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))

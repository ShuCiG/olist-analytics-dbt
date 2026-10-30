"""Deploy transform/ as the DBT PROJECT object OLIST.DBT_PROJECTS.OLIST and its task
graph (snowflake/tasks.sql) (#185).

dbt Projects on Snowflake runs dbt inside the warehouse, from files uploaded to the
project object. Trial accounts allow no external access integration, so `dbt deps`
can't run in Snowflake: the packages go up with the code. The upload is a clean copy
of transform/ (the files Git tracks or would track, so no target/, logs/ or keys)
plus a fresh `dbt deps` from package-lock.yml. Snowflake reads
dbt_projects_profiles.yml (target `native`) instead of profiles.yml.

Each deploy adds a version to the object (ALTER ... ADD VERSION); runs keep their
history. Snowflake compiles the project on deploy, so a broken project fails here.
Then tasks.sql (CREATE OR ALTER, idempotent) updates and resumes the task graph.

Usage: python snowflake/deploy.py
Env:   SNOWFLAKE_ACCOUNT, SNOWFLAKE_PRIVATE_KEY_PATH (OLIST_SVC's key, as for dlt / dbt);
       SNOWFLAKE_USER / _ROLE / _WAREHOUSE / _DATABASE override the defaults
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TRANSFORM = ROOT / "transform"
PROJECT = "OLIST"
SCHEMA = "DBT_PROJECTS"
# Snowflake's managed dbt Core runtime closest to the repo's dbt-core 1.12.6
DBT_VERSION = "1.12.3"
SNOWFLAKE_CLI = "snowflake-cli==3.29.0"


def tracked_files() -> list[str]:
    """transform/ files Git tracks or would track (untracked, not ignored)."""
    out = subprocess.run(
        ["git", "ls-files", "--cached", "--others", "--exclude-standard", "-z", "."],
        cwd=TRANSFORM, check=True, capture_output=True, text=True,
    ).stdout
    return [name for name in out.split("\0") if name and (TRANSFORM / name).is_file()]


def build_source(target: Path) -> None:
    for name in tracked_files():
        (target / name).parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(TRANSFORM / name, target / name)
    subprocess.run(["dbt", "deps", "--project-dir", str(target)], check=True)


def connection() -> list[str]:
    """Snowflake CLI options: key pair of OLIST_SVC, no stored connection."""
    env = os.environ
    return [
        "--temporary-connection",
        "--account", env["SNOWFLAKE_ACCOUNT"],
        "--user", env.get("SNOWFLAKE_USER", "OLIST_SVC"),
        "--authenticator", "SNOWFLAKE_JWT",
        "--private-key-file", os.path.expanduser(env["SNOWFLAKE_PRIVATE_KEY_PATH"]),
        "--role", env.get("SNOWFLAKE_ROLE", "OLIST_RUNNER"),
        "--warehouse", env.get("SNOWFLAKE_WAREHOUSE", "OLIST_WH"),
        "--database", env.get("SNOWFLAKE_DATABASE", "OLIST"),
        "--schema", SCHEMA,
    ]


def snow(*args: str) -> None:
    subprocess.run(["uvx", "--from", SNOWFLAKE_CLI, "snow", *args, *connection()], check=True)


def deploy(source: Path) -> None:
    snow(
        "dbt", "deploy", PROJECT,
        "--source", str(source),
        "--dbt-version", DBT_VERSION,
        "--default-target", "native",
    )


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="olist-dbt-") as tmp:
        source = Path(tmp)
        build_source(source)
        deploy(source)
    snow("sql", "--filename", str(ROOT / "snowflake" / "tasks.sql"))


if __name__ == "__main__":
    sys.exit(main())

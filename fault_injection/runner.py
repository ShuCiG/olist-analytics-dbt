"""Run a scenario: the corrupted dump's next month through the real loader and dbt.

The clean months are loaded and built once. Every scenario starts from a copy of that
warehouse and dlt state, as the feed's next run would, so Elementary compares with a
previous run and anomaly tests have their training data.
"""

import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path

import duckdb

from fault_injection.dump import LAST_CLEAN_MONTH, Rows, clean_rows, write_dump
from fault_injection.scenarios import Scenario

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / "data-product" / "contracts" / "olist-raw.odcs.yaml"
INGEST = """
import sys
from ingestion import pipeline
dump = sys.argv.pop(1)
pipeline.kagglehub.dataset_download = lambda _: dump
pipeline.main()
"""
# Elementary's clock: the last order of the months loaded so far
AS_OF_CLEAN, AS_OF_NEW = "2018-05-31T23:59:59", "2018-06-30T23:59:59"


def db_path(work: Path) -> Path:
    return work / "data" / "olist.duckdb"  # the contract's `local` server reads data/olist.duckdb


def env_for(work: Path, as_of: str) -> dict[str, str]:
    # No API key: the loader would send unlabelled review texts to the Claude API (D92)
    env = {k: v for k, v in os.environ.items() if k not in ("VIRTUAL_ENV", "ANTHROPIC_API_KEY")}
    return env | {
        "DLT_DATA_DIR": str(work / "dlt"),
        "DBT_DUCKDB_PATH": str(db_path(work)),
        "OLIST_AS_OF": as_of,
        "PYTHONPATH": str(ROOT),
        "PYTHONUTF8": "1",
    }


def run(work: Path, as_of: str, cwd: Path, *command: str) -> subprocess.CompletedProcess[str]:
    env = env_for(work, as_of)
    return subprocess.run(command, cwd=cwd, env=env, capture_output=True, text=True, encoding="utf-8", errors="replace")


def ingest(work: Path, dump: Path, as_of: str, *args: str) -> subprocess.CompletedProcess[str]:
    return run(work, as_of, ROOT, sys.executable, "-c", INGEST, str(dump), "--duckdb-path", str(db_path(work)), *args)


def tool(name: str) -> str:
    return str(Path(sys.executable).with_name(name))


def dbt(work: Path, as_of: str, *args: str) -> subprocess.CompletedProcess[str]:
    paths = ("--target-path", str(work / "target"), "--log-path", str(work / "logs"))
    done = run(work, as_of, ROOT / "transform", tool("dbt"), *args, *paths)
    # 1 = nodes failed, the point of a scenario; anything else = dbt itself broke
    assert done.returncode in (0, 1), done.stdout[-4000:] + done.stderr[-4000:]
    return done


def dbt_reactions(work: Path) -> list[str]:
    results = json.loads((work / "target" / "run_results.json").read_text(encoding="utf-8"))["results"]
    return sorted(
        f"{r['status']}:{r['unique_id'].split('.')[2]}" for r in results if r["status"] in ("fail", "warn", "error")
    )


def prepare_baseline(work: Path) -> None:
    db_path(work).parent.mkdir(parents=True)
    dump = write_dump(clean_rows(), work / "kaggle")
    loaded = ingest(work, dump, AS_OF_CLEAN, "--until", LAST_CLEAN_MONTH)
    assert loaded.returncode == 0, loaded.stdout + loaded.stderr
    # Elementary's tables first, as in CI (D38)
    for args in (["run", "--select", "package:elementary"], ["build", "--exclude", "package:elementary"]):
        built = dbt(work, AS_OF_CLEAN, *args)
        assert built.returncode == 0, built.stdout[-4000:]


def corrupted_dump(corrupt: Callable[[Rows], None], directory: Path) -> Path:
    rows = clean_rows()
    corrupt(rows)
    return write_dump(rows, directory)


def copy_baseline(baseline: Path, work: Path) -> None:
    """The warehouse and dlt state after the clean months, for the next month's run."""
    db_path(work).parent.mkdir(parents=True)
    shutil.copy(db_path(baseline), db_path(work))
    shutil.copytree(baseline / "dlt", work / "dlt")
    (work / "target").mkdir()
    shutil.copy(baseline / "target" / "partial_parse.msgpack", work / "target")


def run_scenario(baseline: Path, work: Path, scenario: Scenario) -> tuple[list[str], object]:
    """The reactions, and the value of the scenario's pinned query."""
    copy_baseline(baseline, work)

    loaded = ingest(work, corrupted_dump(scenario.corrupt, work / "kaggle"), AS_OF_NEW, "--next")
    if loaded.returncode != 0:
        return ["loader"], None
    status = json.loads(loaded.stdout.strip().splitlines()[-1])
    reactions = [f"schema:new:{table}" for table in status["new_columns"]]
    reactions += [f"schema:removed:{table}" for table in status["removed_columns"]]
    if scenario.contract:
        checked = run(work, AS_OF_NEW, work, tool("datacontract"), "ci", str(CONTRACT), "--server", "local")
        if checked.returncode != 0:
            reactions.append("contract")
    dbt(work, AS_OF_NEW, "build", "--exclude", "package:elementary")
    reactions += dbt_reactions(work)

    pinned = None
    if scenario.pinned:
        with duckdb.connect(str(db_path(work)), read_only=True) as con:
            row = con.execute(scenario.pinned[0]).fetchone()
        assert row is not None
        pinned = row[0]
    return reactions, pinned

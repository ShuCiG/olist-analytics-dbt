"""Runs jobs as subprocesses and moves them through the state machine.

No shell and no free-form arguments: the command is built from validated fields, and both
the pipeline and dbt run against the local DuckDB file, never a cloud warehouse.
"""

import shutil
import subprocess
import sys
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Protocol

from api.models import JobKind, JobStatus
from api.settings import Settings
from api.store import JobRow, JobStore

TAIL_CHARS = 2000


class Runner(Protocol):
    def submit(self, job_id: str) -> None: ...

    def shutdown(self) -> None: ...


class NotConfigured(Exception):
    """The job needs something this deployment does not have."""


def build_command(job: JobRow, settings: Settings) -> tuple[list[str], Path, str | None]:
    """argv, working directory and the artifact the job leaves behind (repo path)."""
    root = settings.repo_root
    if job.kind is JobKind.pipeline:
        argv = [sys.executable, "ingestion/pipeline.py", "--destination", "duckdb"]
        if job.params.get("until"):
            argv += ["--until", str(job.params["until"])]
        return argv, root, None
    if job.kind is JobKind.dbt_build:
        dbt = shutil.which("dbt")
        if dbt is None:
            raise NotConfigured("dbt is not installed in this environment")
        argv = [dbt, "build"]
        if job.params.get("select"):
            argv += ["--select", str(job.params["select"])]
        return argv, root / "transform", "transform/target/run_results.json"
    if settings.olist_csv_dir is None:
        raise NotConfigured("OLIST_CSV_DIR is not set")
    argv = [sys.executable, "-m", "tsql_parity.run", "--csv-dir", str(settings.olist_csv_dir)]
    if job.params.get("skip_load"):
        argv.append("--skip-load")
    return argv, root, "tsql_parity/report.md"


class SubprocessRunner:
    def __init__(self, store: JobStore, settings: Settings, clock: Callable[[], float] = time.time) -> None:
        self.store = store
        self.settings = settings
        self.clock = clock
        self.pool = ThreadPoolExecutor(max_workers=settings.workers, thread_name_prefix="job")

    def submit(self, job_id: str) -> None:
        self.pool.submit(self._run, job_id)

    def shutdown(self) -> None:
        self.pool.shutdown(wait=False, cancel_futures=True)

    def _run(self, job_id: str) -> None:
        while True:
            job = self.store.transition(job_id, JobStatus.running, now=self.clock())
            result = self._attempt(job)
            if result["exit_code"] == 0:
                self.store.transition(job_id, JobStatus.succeeded, now=self.clock(), result=result)
                return
            if job.attempts >= self.settings.max_attempts:
                self.store.transition(job_id, JobStatus.failed, now=self.clock(), result=result)
                return
            self.store.transition(job_id, JobStatus.queued, now=self.clock())

    def _attempt(self, job: JobRow) -> dict[str, Any]:
        try:
            argv, cwd, artifact = build_command(job, self.settings)
        except NotConfigured as error:
            return {"exit_code": None, "summary": str(error), "output_tail": "", "artifact": None}
        try:
            done = subprocess.run(
                argv,
                cwd=cwd,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=self.settings.job_timeout_seconds,
                check=False,
            )
        except subprocess.TimeoutExpired:
            return {
                "exit_code": None,
                "summary": f"timed out after {self.settings.job_timeout_seconds} s",
                "output_tail": "",
                "artifact": artifact,
            }
        ok = done.returncode == 0
        return {
            "exit_code": done.returncode,
            "summary": f"{job.kind.value} {'succeeded' if ok else 'failed'} (attempt {job.attempts})",
            "output_tail": (done.stdout + done.stderr)[-TAIL_CHARS:],
            "artifact": artifact,
        }

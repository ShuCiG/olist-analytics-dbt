"""Patch Airflow's scheduler: a stale defer-exit success must not fail a resumed task (#285, D115).

A deferred task's worker exits with an executor SUCCESS for its try. When the trigger
fires within seconds, the resumed attempt can be RUNNING (same try_number,
`next_method` set) before the scheduler processes that stale event. Airflow 3.3.2
ignores it only while the TI is SCHEDULED (3.4.0: also QUEUED), so the scheduler logs
"state mismatch" and fails a task that is fine. Upstream: apache/airflow#71580 (open),
fix proposals #71579 and #72767 (closed unmerged) make exactly this change.

Run once in the image build (airflow/Dockerfile). Fails the build when the code to
patch is not there exactly once, so an Airflow upgrade forces a look: drop this script
once a release covers RUNNING. Standard library only.
"""

import sys
from pathlib import Path

MARKER = "# olist patch #285: resumed attempt may already be running"
OLD = """                    ti.state == TaskInstanceState.SCHEDULED
                    and state == TaskInstanceState.SUCCESS
                    and ti.next_method is not None
"""
NEW = f"""                    {MARKER}
                    ti.state
                    in (TaskInstanceState.SCHEDULED, TaskInstanceState.QUEUED, TaskInstanceState.RUNNING)
                    and state == TaskInstanceState.SUCCESS
                    and ti.next_method is not None
"""


def patch(source: str) -> str:
    """The scheduler source with the resume-after-defer check widened; refuses anything unexpected."""
    if MARKER in source:
        raise ValueError("already patched")
    count = source.count(OLD)
    if count != 1:
        raise ValueError(f"expected the resume-after-defer check once, found it {count} times: review #285")
    return source.replace(OLD, NEW)


def main() -> None:
    import airflow.jobs.scheduler_job_runner as module

    assert module.__file__, "scheduler module has no file"
    path = Path(module.__file__)
    path.write_text(patch(path.read_text(encoding="utf-8")), encoding="utf-8")
    print(f"patched {path}")


if __name__ == "__main__":
    try:
        main()
    except ValueError as error:
        sys.exit(f"patch_scheduler_resume: {error}")

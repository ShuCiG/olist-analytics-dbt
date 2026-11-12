"""airflow/patches/scheduler_resume.py: the scheduler fix baked into the image (#285, D115)."""

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
_spec = importlib.util.spec_from_file_location("scheduler_resume", ROOT / "airflow" / "patches" / "scheduler_resume.py")
assert _spec and _spec.loader
scheduler_resume = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(scheduler_resume)

# The block as Airflow 3.3.2 ships it (airflow/jobs/scheduler_job_runner.py, _process_executor_events)
AIRFLOW_332 = """            ti_requeued = (
                ti.queued_by_job_id != job_id  # Another scheduler has queued this task again
                or executor.has_task(ti)  # This scheduler has this task already
                or (
                    # Resume-after-defer: trigger moved TI to scheduled (next_method set) before we saw the
                    # executor success from the defer exit for the same try_number.
                    ti.state == TaskInstanceState.SCHEDULED
                    and state == TaskInstanceState.SUCCESS
                    and ti.next_method is not None
                )
            )
"""


def test_patch_widens_the_resume_check_to_running() -> None:
    patched = scheduler_resume.patch(AIRFLOW_332)
    assert "TaskInstanceState.RUNNING" in patched
    assert scheduler_resume.MARKER in patched
    # Still only a stale SUCCESS of a resumed task (next_method set) is ignored
    assert "and state == TaskInstanceState.SUCCESS\n                    and ti.next_method is not None" in patched
    compile("def f():\n    if True:\n" + patched, "<patched>", "exec")


def test_patch_refuses_changed_code() -> None:
    # An Airflow upgrade that rewrote the block must fail the image build, not skip the fix
    with pytest.raises(ValueError, match="found it 0 times"):
        scheduler_resume.patch(AIRFLOW_332.replace("ti.state == TaskInstanceState.SCHEDULED", "ti.state in (A, B)"))


def test_patch_runs_once() -> None:
    with pytest.raises(ValueError, match="already patched"):
        scheduler_resume.patch(scheduler_resume.patch(AIRFLOW_332))


def test_patch_is_in_the_image() -> None:
    assert "!airflow/patches/" in (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()
    dockerfile = (ROOT / "airflow" / "Dockerfile").read_text(encoding="utf-8")
    assert "RUN python /tmp/scheduler_resume.py" in dockerfile

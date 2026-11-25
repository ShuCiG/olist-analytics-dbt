"""Job state, idempotency keys and used nonces in SQLite.

One connection per call: the file is the only shared state, so two workers (or two requests
at once) meet in the SQLite write lock, not in Python. `BEGIN IMMEDIATE` takes that lock up
front, which is what makes "same key, two requests at once" end in one job.
"""

import json
import sqlite3
import uuid
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from api.models import JobKind, JobStatus

# Forward only. running -> queued is the one way back: a failed attempt that will be retried;
# attempts only ever grows, and succeeded / failed are final.
ALLOWED: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.queued: frozenset({JobStatus.running}),
    JobStatus.running: frozenset({JobStatus.succeeded, JobStatus.failed, JobStatus.queued}),
    JobStatus.succeeded: frozenset(),
    JobStatus.failed: frozenset(),
}

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    id TEXT PRIMARY KEY,
    kind TEXT NOT NULL,
    params TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    created_at REAL NOT NULL,
    started_at REAL,
    finished_at REAL,
    result TEXT
);
CREATE TABLE IF NOT EXISTS idempotency_keys (
    key TEXT PRIMARY KEY,
    body_hash TEXT NOT NULL,
    job_id TEXT NOT NULL REFERENCES jobs (id)
);
CREATE TABLE IF NOT EXISTS nonces (
    nonce TEXT PRIMARY KEY,
    seen_at REAL NOT NULL
);
"""


class IllegalTransition(Exception):
    def __init__(self, job_id: str, old: JobStatus, new: JobStatus) -> None:
        super().__init__(f"job {job_id}: {old.value} -> {new.value} is not allowed")
        self.old = old
        self.new = new


class IdempotencyConflict(Exception):
    """The key was used before with a different request."""


@dataclass(frozen=True)
class JobRow:
    id: str
    kind: JobKind
    params: dict[str, Any]
    status: JobStatus
    attempts: int
    created_at: float
    started_at: float | None
    finished_at: float | None
    result: dict[str, Any] | None


def _row(r: sqlite3.Row) -> JobRow:
    return JobRow(
        id=r["id"],
        kind=JobKind(r["kind"]),
        params=json.loads(r["params"]),
        status=JobStatus(r["status"]),
        attempts=r["attempts"],
        created_at=r["created_at"],
        started_at=r["started_at"],
        finished_at=r["finished_at"],
        result=json.loads(r["result"]) if r["result"] else None,
    )


class JobStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        db = sqlite3.connect(path, timeout=15)
        try:
            db.execute("PRAGMA journal_mode=WAL")  # readers do not block the writer; persists in the file
            db.executescript(SCHEMA)
        finally:
            db.close()

    @contextmanager
    def _tx(self) -> Generator[sqlite3.Connection]:
        db = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        db.row_factory = sqlite3.Row
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.execute("COMMIT")
        except BaseException:
            if db.in_transaction:
                db.execute("ROLLBACK")
            raise
        finally:
            db.close()

    def ping(self) -> None:
        with self._tx() as db:
            db.execute("SELECT 1 FROM jobs LIMIT 1")

    def create_job(
        self, *, key: str, body_hash: str, kind: JobKind, params: dict[str, Any], now: float
    ) -> tuple[JobRow, bool]:
        """The job for this key; True if it was created now. Same key, other body: IdempotencyConflict."""
        with self._tx() as db:
            known = db.execute(
                "SELECT i.body_hash, j.* FROM idempotency_keys i JOIN jobs j ON j.id = i.job_id WHERE i.key = ?",
                (key,),
            ).fetchone()
            if known is not None:
                if known["body_hash"] != body_hash:
                    raise IdempotencyConflict(key)
                return _row(known), False
            job_id = str(uuid.uuid4())
            db.execute(
                "INSERT INTO jobs (id, kind, params, status, created_at) VALUES (?, ?, ?, ?, ?)",
                (job_id, kind.value, json.dumps(params, sort_keys=True), JobStatus.queued.value, now),
            )
            db.execute(
                "INSERT INTO idempotency_keys (key, body_hash, job_id) VALUES (?, ?, ?)", (key, body_hash, job_id)
            )
            return self._get(db, job_id), True

    @staticmethod
    def _get(db: sqlite3.Connection, job_id: str) -> JobRow:
        found = db.execute("SELECT * FROM jobs WHERE id = ?", (job_id,)).fetchone()
        if found is None:
            raise KeyError(job_id)
        return _row(found)

    def get(self, job_id: str) -> JobRow | None:
        with self._tx() as db:
            try:
                return self._get(db, job_id)
            except KeyError:
                return None

    def transition(
        self, job_id: str, new: JobStatus, *, now: float, result: dict[str, Any] | None = None
    ) -> JobRow:
        """Move a job along ALLOWED; KeyError for an unknown job, IllegalTransition otherwise."""
        with self._tx() as db:
            current = self._get(db, job_id)
            if new not in ALLOWED[current.status]:
                raise IllegalTransition(job_id, current.status, new)
            if new is JobStatus.running:
                db.execute(
                    "UPDATE jobs SET status = ?, attempts = attempts + 1, started_at = ? WHERE id = ?",
                    (new.value, now, job_id),
                )
            elif new is JobStatus.queued:
                db.execute("UPDATE jobs SET status = ? WHERE id = ?", (new.value, job_id))
            else:
                db.execute(
                    "UPDATE jobs SET status = ?, finished_at = ?, result = ? WHERE id = ?",
                    (new.value, now, json.dumps(result) if result is not None else None, job_id),
                )
            return self._get(db, job_id)

    def unfinished(self) -> list[JobRow]:
        with self._tx() as db:
            rows = db.execute(
                "SELECT * FROM jobs WHERE status IN (?, ?) ORDER BY created_at",
                (JobStatus.queued.value, JobStatus.running.value),
            ).fetchall()
        return [_row(r) for r in rows]

    def use_nonce(self, nonce: str, *, now: float, window_seconds: int) -> bool:
        """Record a nonce; False if it was seen inside the window (a replay)."""
        with self._tx() as db:
            # A nonce older than the window cannot pass the timestamp check any more
            db.execute("DELETE FROM nonces WHERE seen_at < ?", (now - 2 * window_seconds,))
            seen = db.execute("SELECT 1 FROM nonces WHERE nonce = ?", (nonce,)).fetchone()
            if seen is not None:
                return False
            db.execute("INSERT INTO nonces (nonce, seen_at) VALUES (?, ?)", (nonce, now))
            return True

"""FastAPI app. Start: `uv run uvicorn api.app:create_app --factory` (see api/README.md)."""

import hashlib
import json
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, Request, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.security import APIKeyHeader

from api import problems, signing
from api.models import Health, Job, JobKind, JobRequest, JobResult, JobStatus, ParityRequest, Problem
from api.problems import ApiProblem
from api.runner import Runner, SubprocessRunner
from api.settings import Settings
from api.store import IdempotencyConflict, JobRow, JobStore

API_VERSION = "1.0.0"
IDEMPOTENCY_KEY = Header(
    alias="Idempotency-Key",
    min_length=1,
    max_length=128,
    pattern=r"^[A-Za-z0-9._~-]+$",
    description="Client-chosen key; the same key with the same body returns the same job",
)

signature_scheme = APIKeyHeader(
    name=signing.SIGNATURE_HEADER,
    scheme_name="HMACSignature",
    auto_error=False,
    description=(
        "Hex HMAC-SHA256 over timestamp, nonce, method, path with query and raw body, joined by "
        "newlines, with the headers X-Timestamp (unix seconds, 5 minute window) and X-Nonce "
        "(16-64 characters, used once)."
    ),
)


def _problem_responses(*statuses: int) -> dict[int | str, dict[str, Any]]:
    return {status: {"model": Problem, "description": "Problem details"} for status in statuses}


def _utc(timestamp: float | None) -> datetime | None:
    return datetime.fromtimestamp(timestamp, UTC) if timestamp is not None else None


def _job(row: JobRow) -> Job:
    return Job(
        id=row.id,
        kind=row.kind,
        status=row.status,
        attempts=row.attempts,
        created_at=datetime.fromtimestamp(row.created_at, UTC),
        started_at=_utc(row.started_at),
        finished_at=_utc(row.finished_at),
        params=row.params,
    )


def _body_hash(endpoint: str, body: dict[str, Any]) -> str:
    """Same request = same hash, whatever the client's key order or whitespace."""
    canonical = json.dumps({"endpoint": endpoint, "body": body}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


def create_app(
    settings: Settings | None = None,
    runner: Runner | None = None,
    clock: Callable[[], float] = time.time,
) -> FastAPI:
    settings = settings or Settings.from_env()
    store = JobStore(settings.db_path)
    runner = runner or SubprocessRunner(store, settings, clock)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncGenerator[None]:
        # A restart leaves jobs half done: what was running is lost, what was queued runs again
        for job in store.unfinished():
            if job.status is JobStatus.running:
                store.transition(
                    job.id,
                    JobStatus.failed,
                    now=clock(),
                    result={
                        "exit_code": None,
                        "summary": "interrupted by a service restart",
                        "output_tail": "",
                        "artifact": None,
                    },
                )
            else:
                runner.submit(job.id)
        yield
        runner.shutdown()

    app = FastAPI(
        title="Olist job control",
        version=API_VERSION,
        description="Start and follow pipeline runs, dbt builds and the T-SQL / Spark parity check.",
        lifespan=lifespan,
    )
    problems.install(app)
    app.state.store = store

    async def signed(request: Request, _: Annotated[str | None, Depends(signature_scheme)] = None) -> None:
        body = await request.body()
        target = request.url.path + (f"?{request.url.query}" if request.url.query else "")
        now = clock()
        try:
            nonce = signing.verify(
                settings.hmac_secret,
                request.headers,
                request.method,
                target,
                body,
                now=now,
                window_seconds=settings.replay_window_seconds,
            )
        except signing.SignatureError as error:
            raise ApiProblem(
                401, error.slug, "Unauthorized", error.detail, headers={"WWW-Authenticate": "HMAC"}
            ) from error
        fresh = await run_in_threadpool(
            store.use_nonce, nonce, now=now, window_seconds=settings.replay_window_seconds
        )
        if not fresh:
            raise ApiProblem(
                401, "replayed-nonce", "Unauthorized", "this nonce was used before", headers={"WWW-Authenticate": "HMAC"}
            )

    auth = [Depends(signed)]
    auth_errors = _problem_responses(401)

    def start(
        response: Response, *, key: str, endpoint: str, body: dict[str, Any], kind: JobKind, params: dict[str, Any]
    ) -> Job:
        try:
            row, created = store.create_job(
                key=key, body_hash=_body_hash(endpoint, body), kind=kind, params=params, now=clock()
            )
        except IdempotencyConflict as error:
            raise ApiProblem(
                409, "idempotency-key-reused", "Conflict", "this Idempotency-Key was used with a different request"
            ) from error
        if created:
            runner.submit(row.id)
        else:
            response.headers["Idempotent-Replayed"] = "true"
        return _job(row)

    @app.post(
        "/v1/jobs",
        status_code=202,
        response_model=Job,
        dependencies=auth,
        responses={**auth_errors, **_problem_responses(409, 422)},
        tags=["jobs"],
        summary="Start a pipeline run or a dbt build",
    )
    def create_job(
        request: JobRequest, response: Response, idempotency_key: Annotated[str, IDEMPOTENCY_KEY]
    ) -> Job:
        body = request.model_dump(mode="json")
        params = {name: value for name, value in body.items() if name != "kind" and value is not None}
        return start(
            response,
            key=idempotency_key,
            endpoint="/v1/jobs",
            body=body,
            kind=JobKind(request.kind),
            params=params,
        )

    @app.post(
        "/v1/parity/run",
        status_code=202,
        response_model=Job,
        dependencies=auth,
        responses={**auth_errors, **_problem_responses(409, 422, 503)},
        tags=["jobs"],
        summary="Start the T-SQL / Spark parity check",
    )
    def run_parity(
        request: ParityRequest, response: Response, idempotency_key: Annotated[str, IDEMPOTENCY_KEY]
    ) -> Job:
        if settings.olist_csv_dir is None:
            raise ApiProblem(503, "parity-not-configured", "Service Unavailable", "OLIST_CSV_DIR is not set")
        body = request.model_dump(mode="json")
        return start(
            response, key=idempotency_key, endpoint="/v1/parity/run", body=body, kind=JobKind.parity, params=body
        )

    def existing(job_id: str) -> JobRow:
        row = store.get(job_id)
        if row is None:
            raise ApiProblem(404, "job-not-found", "Not Found", f"no job {job_id}")
        return row

    @app.get(
        "/v1/jobs/{job_id}",
        response_model=Job,
        dependencies=auth,
        responses={**auth_errors, **_problem_responses(404)},
        tags=["jobs"],
        summary="Job status",
    )
    def get_job(job_id: str) -> Job:
        return _job(existing(job_id))

    @app.get(
        "/v1/jobs/{job_id}/result",
        response_model=JobResult,
        dependencies=auth,
        responses={**auth_errors, **_problem_responses(404, 409)},
        tags=["jobs"],
        summary="Outcome of a finished job",
    )
    def get_result(job_id: str) -> JobResult:
        row = existing(job_id)
        if row.status not in (JobStatus.succeeded, JobStatus.failed) or row.result is None:
            raise ApiProblem(409, "job-not-finished", "Conflict", f"job {job_id} is {row.status.value}")
        return JobResult(
            job_id=row.id,
            status=row.status,  # pyright: ignore[reportArgumentType]  # narrowed to the two final states above
            exit_code=row.result["exit_code"],
            summary=row.result["summary"],
            output_tail=row.result["output_tail"],
            artifact=row.result["artifact"],
        )

    @app.get("/healthz", response_model=Health, tags=["ops"], summary="Process is up")
    def healthz() -> Health:
        return Health(status="ok")

    @app.get(
        "/readyz",
        response_model=Health,
        responses=_problem_responses(503),
        tags=["ops"],
        summary="Job store is reachable",
    )
    def readyz() -> Health:
        try:
            store.ping()
        except Exception as error:
            raise ApiProblem(503, "not-ready", "Service Unavailable", "the job store is not reachable") from error
        return Health(status="ok")

    return app

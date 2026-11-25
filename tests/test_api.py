"""Job-control service (api/): signing, idempotency, state machine, problem+json, OpenAPI check.

The app runs in process behind httpx's ASGI transport; a fake runner records what would start,
so no pipeline or dbt process runs here (one test drives the real runner with a stub command).
"""

import asyncio
import json
import sys
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import httpx
import pytest
from fastapi import FastAPI

from api import check_openapi, openapi
from api import runner as runner_module
from api.app import create_app
from api.models import JobKind, JobStatus
from api.runner import SubprocessRunner
from api.settings import Settings
from api.signing import NONCE_HEADER, SIGNATURE_HEADER, TIMESTAMP_HEADER, signed_headers
from api.store import IllegalTransition, JobStore

SECRET = b"unit-test-secret-unit-test-secret-0123456789"
NOW = 1_800_000_000.0


class FakeRunner:
    def __init__(self) -> None:
        self.submitted: list[str] = []

    def submit(self, job_id: str) -> None:
        self.submitted.append(job_id)

    def shutdown(self) -> None:
        pass


class Clock:
    def __init__(self) -> None:
        self.now = NOW

    def __call__(self) -> float:
        return self.now


class Api:
    """The app plus a client that signs like a real caller."""

    def __init__(self, tmp_path: Path, **settings: Any) -> None:
        self.settings = Settings(hmac_secret=SECRET, db_path=tmp_path / "jobs.sqlite3", **settings)
        self.clock = Clock()
        self.runner = FakeRunner()
        self.app: FastAPI = create_app(self.settings, runner=self.runner, clock=self.clock)
        self.store: JobStore = self.app.state.store
        self.client = httpx.AsyncClient(transport=httpx.ASGITransport(app=self.app), base_url="http://api.test")

    async def call(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        *,
        key: str | None = None,
        headers: dict[str, str] | None = None,
        secret: bytes = SECRET,
        at: float | None = None,
        nonce: str | None = None,
    ) -> httpx.Response:
        raw = json.dumps(body).encode() if body is not None else b""
        sent = signed_headers(secret, method, path, raw, now=self.clock.now if at is None else at, nonce=nonce)
        if raw:
            sent["Content-Type"] = "application/json"
        if key:
            sent["Idempotency-Key"] = key
        sent.update(headers or {})
        return await self.client.request(method, path, content=raw, headers=sent)

    async def start(self, key: str = "key-1", body: dict[str, Any] | None = None) -> httpx.Response:
        return await self.call("POST", "/v1/jobs", body or {"kind": "pipeline"}, key=key)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@pytest.fixture
def api(tmp_path: Path) -> Iterator[Api]:
    yield Api(tmp_path)


pytestmark = pytest.mark.anyio


async def test_start_job_answers_202_and_hands_it_to_the_runner(api: Api):
    response = await api.start(body={"kind": "dbt_build", "select": "marts"})
    assert response.status_code == 202
    job = response.json()
    assert (job["kind"], job["status"], job["attempts"]) == ("dbt_build", "queued", 0)
    assert job["params"] == {"select": "marts"}
    assert api.runner.submitted == [job["id"]]


async def test_same_key_same_body_returns_the_same_job(api: Api):
    first = await api.start(body={"kind": "pipeline", "until": "2017-03"})
    # Another key order and whitespace are the same request
    second = await api.call("POST", "/v1/jobs", {"until": "2017-03", "kind": "pipeline"}, key="key-1")
    assert second.status_code == 202
    assert second.json()["id"] == first.json()["id"]
    assert second.headers["Idempotent-Replayed"] == "true"
    assert "Idempotent-Replayed" not in first.headers
    assert api.runner.submitted == [first.json()["id"]]


async def test_same_key_other_body_is_409(api: Api):
    await api.start(body={"kind": "pipeline"})
    response = await api.start(body={"kind": "dbt_build"})
    assert response.status_code == 409
    assert response.headers["content-type"] == "application/problem+json"
    assert response.json()["type"] == "urn:olist-api:problem:idempotency-key-reused"
    assert len(api.runner.submitted) == 1


async def test_same_key_on_another_endpoint_is_409(api: Api):
    api_with_csv = Api(api.settings.db_path.parent / "other", olist_csv_dir=Path("."))
    await api_with_csv.start(key="shared", body={"kind": "pipeline"})
    response = await api_with_csv.call("POST", "/v1/parity/run", {}, key="shared")
    assert response.status_code == 409


async def test_two_requests_at_once_with_one_key_make_one_job(api: Api):
    responses = await asyncio.gather(*(api.start(key="race") for _ in range(8)))
    assert {r.status_code for r in responses} == {202}
    assert len({r.json()["id"] for r in responses}) == 1
    assert len(api.runner.submitted) == 1
    assert sum("Idempotent-Replayed" not in r.headers for r in responses) == 1


async def test_two_requests_at_once_with_one_key_and_different_bodies_make_one_job(api: Api):
    bodies = [{"kind": "pipeline", "until": f"2017-0{n}"} for n in range(1, 5)]
    responses = await asyncio.gather(*(api.start(key="race", body=b) for b in bodies))
    assert sorted(r.status_code for r in responses) == [202, 409, 409, 409]
    assert len(api.runner.submitted) == 1


async def test_expired_timestamp_is_401(api: Api):
    response = await api.call("GET", "/v1/jobs/x", at=NOW - 301)
    assert response.status_code == 401
    assert response.json()["type"] == "urn:olist-api:problem:stale-timestamp"
    assert response.headers["www-authenticate"] == "HMAC"
    assert (await api.call("GET", "/v1/jobs/x", at=NOW + 301)).status_code == 401
    # Inside the window the request gets as far as the lookup
    assert (await api.call("GET", "/v1/jobs/x", at=NOW - 299)).status_code == 404


async def test_forged_signature_is_401(api: Api):
    wrong_secret = await api.call("GET", "/v1/jobs/x", secret=b"not-the-secret-not-the-secret-not-the-secret")
    assert wrong_secret.json()["type"].endswith("bad-signature")
    # Signed for one body, sent with another
    headers = signed_headers(SECRET, "POST", "/v1/jobs", b'{"kind": "pipeline"}', now=NOW)
    tampered = await api.client.post(
        "/v1/jobs", content=b'{"kind": "dbt_build"}', headers={**headers, "Idempotency-Key": "k"}
    )
    assert tampered.status_code == 401
    # Signed for one path, sent to another
    headers = signed_headers(SECRET, "GET", "/healthz", b"", now=NOW)
    assert (await api.client.get("/v1/jobs/x", headers=headers)).status_code == 401
    # Right length, not hex
    garbage = await api.call("GET", "/v1/jobs/x", headers={SIGNATURE_HEADER: "Z" * 64})
    assert garbage.json()["type"].endswith("bad-signature")
    assert api.store.ping() is None
    assert api.runner.submitted == []


async def test_missing_signature_headers_are_401_not_422(api: Api):
    response = await api.client.post("/v1/jobs", json={"kind": "pipeline"})
    assert response.status_code == 401
    assert response.json()["type"].endswith("missing-signature")


async def test_a_nonce_works_once(api: Api):
    nonce = "a-fixed-nonce-0123456789"
    assert (await api.call("GET", "/v1/jobs/x", nonce=nonce)).status_code == 404
    again = await api.call("GET", "/v1/jobs/x", nonce=nonce)
    assert again.status_code == 401
    assert again.json()["type"].endswith("replayed-nonce")


async def test_an_unsigned_caller_cannot_burn_nonces(api: Api):
    nonce = "a-fixed-nonce-0123456789"
    forged = await api.call("GET", "/v1/jobs/x", nonce=nonce, secret=b"x" * 40)
    assert forged.status_code == 401
    assert (await api.call("GET", "/v1/jobs/x", nonce=nonce)).status_code == 404


async def test_old_nonces_are_forgotten(api: Api):
    assert api.store.use_nonce("n" * 20, now=NOW, window_seconds=300)
    assert not api.store.use_nonce("n" * 20, now=NOW + 599, window_seconds=300)
    assert api.store.use_nonce("n" * 20, now=NOW + 601, window_seconds=300)


async def test_probes_need_no_signature(api: Api):
    assert (await api.client.get("/healthz")).json() == {"status": "ok"}
    assert (await api.client.get("/readyz")).json() == {"status": "ok"}


async def test_errors_are_problem_json(api: Api):
    missing = await api.call("GET", "/v1/jobs/nope")
    assert missing.status_code == 404
    assert missing.headers["content-type"] == "application/problem+json"
    assert set(missing.json()) == {"type", "title", "status", "detail", "instance"}
    assert missing.json()["instance"] == "/v1/jobs/nope"

    invalid = await api.start(body={"kind": "pipeline", "select": "marts"})
    assert invalid.status_code == 422
    assert invalid.headers["content-type"] == "application/problem+json"
    assert invalid.json()["errors"][0]["message"].startswith("Value error")

    no_key = await api.call("POST", "/v1/jobs", {"kind": "pipeline"})
    assert no_key.status_code == 422
    assert no_key.json()["errors"][0]["where"] == "header.Idempotency-Key"

    assert (await api.start(body={"kind": "pipeline", "select": "--profiles-dir"})).status_code == 422
    assert (await api.start(body={"kind": "dbt_build", "select": "--vars"})).status_code == 422
    assert (await api.start(body={"kind": "pipeline", "until": "2017-13"})).status_code == 422
    assert (await api.start(body={"kind": "pipeline", "target": "prod"})).status_code == 422
    assert (await api.call("DELETE", "/v1/jobs/x")).headers["content-type"] == "application/problem+json"


async def test_parity_needs_the_dump_folder(api: Api):
    response = await api.call("POST", "/v1/parity/run", {}, key="p1")
    assert response.status_code == 503
    assert response.headers["content-type"] == "application/problem+json"

    with_csv = Api(api.settings.db_path.parent / "csv", olist_csv_dir=Path("dump"))
    started = await with_csv.call("POST", "/v1/parity/run", {"skip_load": True}, key="p1")
    assert started.status_code == 202
    assert (started.json()["kind"], started.json()["params"]) == ("parity", {"skip_load": True})


async def test_status_and_result_follow_the_job(api: Api):
    job_id = (await api.start()).json()["id"]
    assert (await api.call("GET", f"/v1/jobs/{job_id}")).json()["status"] == "queued"
    assert (await api.call("GET", f"/v1/jobs/{job_id}/result")).status_code == 409

    api.clock.now += 10
    api.store.transition(job_id, JobStatus.running, now=api.clock.now)
    running = (await api.call("GET", f"/v1/jobs/{job_id}")).json()
    assert (running["status"], running["attempts"]) == ("running", 1)
    assert running["started_at"].startswith("2027-01-15T08:00:10")
    assert running["finished_at"] is None

    api.clock.now += 5
    result = {"exit_code": 0, "summary": "ok", "output_tail": "done", "artifact": "transform/target/run_results.json"}
    api.store.transition(job_id, JobStatus.succeeded, now=api.clock.now, result=result)
    done = (await api.call("GET", f"/v1/jobs/{job_id}")).json()
    assert (done["status"], done["finished_at"].startswith("2027-01-15T08:00:15")) == ("succeeded", True)
    assert (await api.call("GET", f"/v1/jobs/{job_id}/result")).json() == {"job_id": job_id, "status": "succeeded", **result}


@pytest.mark.parametrize(
    ("path", "new"),
    [
        ([], JobStatus.succeeded),  # queued -> succeeded: never ran
        ([], JobStatus.failed),
        ([JobStatus.running, JobStatus.succeeded], JobStatus.running),  # a finished job does not restart
        ([JobStatus.running, JobStatus.succeeded], JobStatus.queued),
        ([JobStatus.running, JobStatus.failed], JobStatus.queued),
        ([JobStatus.running, JobStatus.failed], JobStatus.succeeded),
        ([JobStatus.running], JobStatus.running),
    ],
)
async def test_illegal_status_transitions_are_refused(api: Api, path: list[JobStatus], new: JobStatus):
    job_id = (await api.start()).json()["id"]
    for step in path:
        api.store.transition(job_id, step, now=NOW)
    before = api.store.get(job_id)
    with pytest.raises(IllegalTransition):
        api.store.transition(job_id, new, now=NOW + 1)
    assert api.store.get(job_id) == before


async def test_a_retry_counts_attempts_and_never_loses_them(api: Api):
    job_id = (await api.start()).json()["id"]
    for _ in range(2):
        api.store.transition(job_id, JobStatus.running, now=NOW)
        api.store.transition(job_id, JobStatus.queued, now=NOW)
    job = api.store.get(job_id)
    assert job is not None
    assert (job.status, job.attempts) == (JobStatus.queued, 2)


async def test_restart_fails_running_jobs_and_requeues_queued_ones(api: Api):
    running = (await api.start(key="a")).json()["id"]
    queued = (await api.start(key="b")).json()["id"]
    api.store.transition(running, JobStatus.running, now=NOW)
    api.runner.submitted.clear()

    async with api.app.router.lifespan_context(api.app):
        pass
    lost = api.store.get(running)
    assert lost is not None
    assert lost.status is JobStatus.failed
    assert lost.result is not None
    assert lost.result["summary"] == "interrupted by a service restart"
    assert api.runner.submitted == [queued]


async def test_runner_retries_a_failed_command_then_gives_up(api: Api, monkeypatch: pytest.MonkeyPatch):
    stub = ([sys.executable, "-c", "import sys; print('boom'); sys.exit(3)"], Path.cwd(), "some/artifact")
    monkeypatch.setattr(runner_module, "build_command", lambda job, settings: stub)  # pyright: ignore[reportUnknownLambdaType]
    job_id = (await api.start()).json()["id"]

    SubprocessRunner(api.store, api.settings, api.clock)._run(job_id)  # pyright: ignore[reportPrivateUsage]
    job = api.store.get(job_id)
    assert job is not None
    assert (job.status, job.attempts) == (JobStatus.failed, api.settings.max_attempts)
    assert job.result is not None
    assert (job.result["exit_code"], job.result["output_tail"].strip()) == (3, "boom")
    assert (await api.call("GET", f"/v1/jobs/{job_id}/result")).json()["status"] == "failed"


async def test_runner_marks_a_working_command_succeeded(api: Api, monkeypatch: pytest.MonkeyPatch):
    stub = ([sys.executable, "-c", "print('fine')"], Path.cwd(), None)
    monkeypatch.setattr(runner_module, "build_command", lambda job, settings: stub)  # pyright: ignore[reportUnknownLambdaType]
    job_id = (await api.start()).json()["id"]
    SubprocessRunner(api.store, api.settings, api.clock)._run(job_id)  # pyright: ignore[reportPrivateUsage]
    job = api.store.get(job_id)
    assert job is not None
    assert (job.status, job.attempts) == (JobStatus.succeeded, 1)


def test_commands_come_from_validated_fields_only(tmp_path: Path):
    settings = Settings(hmac_secret=SECRET, db_path=tmp_path / "j.sqlite3", olist_csv_dir=Path("dump"), repo_root=tmp_path)
    store = JobStore(settings.db_path)
    row, _ = store.create_job(
        key="k", body_hash="h", kind=JobKind.pipeline, params={"until": "2017-03"}, now=NOW
    )
    argv, cwd, artifact = runner_module.build_command(row, settings)
    assert argv[1:] == ["ingestion/pipeline.py", "--destination", "duckdb", "--until", "2017-03"]
    assert (cwd, artifact) == (tmp_path, None)

    parity, _ = store.create_job(key="p", body_hash="h", kind=JobKind.parity, params={"skip_load": True}, now=NOW)
    argv, _, artifact = runner_module.build_command(parity, settings)
    assert argv[1:] == ["-m", "tsql_parity.run", "--csv-dir", "dump", "--skip-load"]
    assert artifact == "tsql_parity/report.md"


def test_settings_refuse_a_short_or_missing_secret():
    with pytest.raises(ValueError, match="at least 32"):
        Settings(hmac_secret=b"short")
    with pytest.raises(ValueError, match="API_HMAC_SECRET is not set"):
        Settings.from_env({})
    assert Settings.from_env({"API_HMAC_SECRET": "s" * 40, "API_MAX_ATTEMPTS": "5"}).max_attempts == 5


# --- OpenAPI schema -------------------------------------------------------------------------


def test_the_checked_in_schema_is_the_apps_schema():
    assert openapi.main.__module__ == "api.openapi"
    assert openapi.SCHEMA_PATH.read_text(encoding="utf-8") == openapi.render(openapi.build_schema()), (
        "api/openapi.json is out of date: run `uv run python -m api.openapi`"
    )


def test_the_schema_documents_signing_and_problem_responses():
    schema = openapi.build_schema()
    assert schema["components"]["securitySchemes"]["HMACSignature"]["name"] == SIGNATURE_HEADER
    create = schema["paths"]["/v1/jobs"]["post"]
    assert create["responses"]["409"]["content"]["application/problem+json"]["schema"] == {
        "$ref": "#/components/schemas/Problem"
    }
    assert create["security"] == [{"HMACSignature": []}]
    assert "security" not in schema["paths"]["/healthz"]["get"]
    assert {TIMESTAMP_HEADER, NONCE_HEADER} <= set(schema["components"]["securitySchemes"]["HMACSignature"]["description"].split())


@pytest.fixture
def old_schema() -> dict[str, Any]:
    return json.loads(openapi.SCHEMA_PATH.read_text(encoding="utf-8"))


def changed(schema: dict[str, Any], edit: Any) -> dict[str, Any]:
    new = json.loads(json.dumps(schema))
    edit(new)
    return new


def test_no_change_is_not_breaking(old_schema: dict[str, Any]):
    assert check_openapi.verdict(old_schema, old_schema) == []


def test_additions_are_not_breaking(old_schema: dict[str, Any]):
    def edit(s: dict[str, Any]) -> None:
        s["components"]["schemas"]["Job"]["properties"]["owner"] = {"type": "string"}
        s["components"]["schemas"]["JobRequest"]["properties"]["note"] = {"type": "string"}
        s["paths"]["/v1/jobs/{job_id}/cancel"] = {"post": {"responses": {"202": {"description": "ok"}}}}

    assert check_openapi.breaking_changes(old_schema, changed(old_schema, edit)) == []


@pytest.mark.parametrize(
    ("edit", "expected"),
    [
        (lambda s: s["paths"].pop("/v1/jobs"), "POST /v1/jobs: removed"),
        (lambda s: s["paths"]["/v1/jobs/{job_id}"]["get"]["responses"].pop("404"), "response 404 removed"),
        (lambda s: s["components"]["schemas"]["Job"]["properties"].pop("status"), "GET /v1/jobs/{job_id} 200.status: property removed"),
        (lambda s: s["components"]["schemas"]["Job"]["properties"]["attempts"].update(type="string"), "type ['integer'] -> ['string']"),
        (lambda s: s["components"]["schemas"]["JobRequest"]["required"].append("until"), "request.until: now required"),
        (lambda s: s["components"]["schemas"]["JobRequest"]["properties"].pop("select"), "request.select: property removed"),
        (lambda s: s["components"]["schemas"]["JobStatus"]["enum"].append("cancelled"), "enum value(s) added: ['cancelled']"),
        (lambda s: s["components"]["schemas"]["JobKind"]["enum"].remove("parity"), None),  # response-only: not breaking
        (
            lambda s: s["paths"]["/v1/jobs"]["post"].setdefault("parameters", []).append(
                {"name": "tenant", "in": "query", "required": True, "schema": {"type": "string"}}
            ),
            "parameter tenant (query) is now required",
        ),
    ],
)
def test_breaking_changes_are_found(old_schema: dict[str, Any], edit: Any, expected: str | None):
    found = check_openapi.breaking_changes(old_schema, changed(old_schema, edit))
    if expected is None:
        assert found == []
    else:
        assert any(expected in line for line in found), found


def test_a_breaking_change_needs_a_new_major_version(old_schema: dict[str, Any]):
    def drop_path(s: dict[str, Any]) -> None:
        s["paths"].pop("/v1/jobs")

    breaking = changed(old_schema, drop_path)
    assert check_openapi.verdict(old_schema, breaking)[0].startswith("breaking change without a new major version")
    breaking["info"]["version"] = "2.0.0"
    assert check_openapi.verdict(old_schema, breaking) == []

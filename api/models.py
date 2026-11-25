"""Request and response models: everything the API accepts or returns is declared here."""

from datetime import datetime
from enum import StrEnum
from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class JobKind(StrEnum):
    pipeline = "pipeline"
    dbt_build = "dbt_build"
    parity = "parity"


class JobStatus(StrEnum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"


class JobRequest(BaseModel):
    """Start a pipeline run or a dbt build. Both run against the local DuckDB file only."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["pipeline", "dbt_build"]
    until: str | None = Field(
        default=None,
        pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
        description="pipeline only: load order months up to this one, YYYY-MM (default: all)",
    )
    select: str | None = Field(
        default=None,
        pattern=r"^[A-Za-z0-9_][A-Za-z0-9_.+:*,@/-]{0,99}$",
        description="dbt_build only: a dbt node selection, e.g. `marts` or `+fct_orders`",
    )

    @model_validator(mode="after")
    def only_own_fields(self) -> Self:
        if self.kind == "pipeline" and self.select is not None:
            raise ValueError("`select` belongs to kind dbt_build")
        if self.kind == "dbt_build" and self.until is not None:
            raise ValueError("`until` belongs to kind pipeline")
        return self


class ParityRequest(BaseModel):
    """Start the T-SQL / Spark parity check (`tsql_parity/`)."""

    model_config = ConfigDict(extra="forbid")

    skip_load: bool = Field(default=False, description="SQL Server tables are already loaded")


class Job(BaseModel):
    id: str
    kind: JobKind
    status: JobStatus
    attempts: int = Field(description="how many times the job started running; a failed run is retried")
    created_at: datetime
    started_at: datetime | None = Field(description="start of the latest attempt")
    finished_at: datetime | None = None
    params: dict[str, str | bool | None]


class JobResult(BaseModel):
    job_id: str
    status: Literal[JobStatus.succeeded, JobStatus.failed]
    exit_code: int | None
    summary: str
    output_tail: str = Field(description="last lines of the job's output")
    artifact: str | None = Field(description="repo path of the file the job leaves behind, if any")


class Problem(BaseModel):
    """RFC 7807 problem details (`application/problem+json`)."""

    type: str
    title: str
    status: int
    detail: str
    instance: str


class Health(BaseModel):
    status: Literal["ok"]

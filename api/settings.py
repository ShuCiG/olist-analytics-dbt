"""Service settings, read from the environment once at start-up (`api/.env.example`)."""

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
MIN_SECRET_BYTES = 32


@dataclass(frozen=True)
class Settings:
    hmac_secret: bytes
    db_path: Path = REPO / "data" / "api_jobs.sqlite3"
    replay_window_seconds: int = 300
    max_attempts: int = 2
    job_timeout_seconds: int = 3600
    workers: int = 2
    # Dump folder for the parity run; without it POST /v1/parity/run answers 503
    olist_csv_dir: Path | None = None
    repo_root: Path = REPO

    def __post_init__(self) -> None:
        if len(self.hmac_secret) < MIN_SECRET_BYTES:
            raise ValueError(f"API_HMAC_SECRET must be at least {MIN_SECRET_BYTES} bytes")

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> "Settings":
        env = os.environ if env is None else env
        secret = env.get("API_HMAC_SECRET")
        if not secret:
            raise ValueError("API_HMAC_SECRET is not set (see api/.env.example)")
        csv_dir = env.get("OLIST_CSV_DIR")
        return cls(
            hmac_secret=secret.encode(),
            db_path=Path(env.get("API_DB_PATH", str(cls.db_path))),
            replay_window_seconds=int(env.get("API_REPLAY_WINDOW_SECONDS", cls.replay_window_seconds)),
            max_attempts=int(env.get("API_MAX_ATTEMPTS", cls.max_attempts)),
            job_timeout_seconds=int(env.get("API_JOB_TIMEOUT_SECONDS", cls.job_timeout_seconds)),
            workers=int(env.get("API_WORKERS", cls.workers)),
            olist_csv_dir=Path(csv_dir) if csv_dir else None,
        )

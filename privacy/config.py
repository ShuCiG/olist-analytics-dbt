"""Release thresholds, read from `privacy/config.toml`."""

import tomllib
from dataclasses import dataclass
from pathlib import Path

DEFAULT_PATH = Path(__file__).with_name("config.toml")


@dataclass(frozen=True)
class PrivacyConfig:
    k: int = 5  # a group must hold at least k distinct people
    l: int = 2  # a group must show at least l distinct values of a sensitive column  # noqa: E741

    def __post_init__(self) -> None:
        if self.k < 2:
            raise ValueError("k must be at least 2 (k = 1 protects nobody)")
        if self.l < 1:
            raise ValueError("l must be at least 1")


def load_config(path: Path = DEFAULT_PATH) -> PrivacyConfig:
    with path.open("rb") as fh:
        data = tomllib.load(fh)
    unknown = set(data) - {"k", "l"}
    if unknown:
        raise ValueError(f"unknown keys in {path.name}: {sorted(unknown)}")
    return PrivacyConfig(**{key: int(value) for key, value in data.items()})

"""Settings and sanitized errors for local batch loading."""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from spartment.common.config import REPO_ROOT


class LoadError(RuntimeError):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass
class Settings:
    dsn: str = field(repr=False)
    root: Path = REPO_ROOT
    pg_bin: Path | None = None
    owner_dsn: str | None = field(default=None, repr=False)
    budget_seconds: int = 1800
    warning_seconds: int = 1200
    min_free_bytes: int = 1024**3
    warning_free_ratio: float = 0.20
    lock_seconds: int = 30
    target_profile: str = "auto"
    capacity_mode: str = "auto"
    database_budget_bytes: int = 450_000_000
    hosted_reserve_bytes: int = 16_000_000
    capacity_evidence: Path | None = None
    # Required when a non-owner cannot discover the server's filesystem paths.
    storage_paths: tuple[Path, ...] = ()


@dataclass
class Package:
    handover: dict[str, Any]
    rows: dict[str, list[tuple[Any, ...]]]
    counts: dict[str, int]
    folder: Path
    bytes_received: int


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    import json
    import os
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(path.name + ".tmp")
    temp.write_text(json.dumps(data, indent=2, default=str) + "\n", encoding="utf-8")
    os.replace(temp, path)

"""Runtime configuration for the Spartment pipeline.

Settings come from two places. Non secret values such as page size and
retention periods come from ``config/extraction.toml``. Credentials come from
the environment and are never written to any record, log or staged dataset.
"""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

PACKAGE_ROOT = Path(__file__).resolve().parent
DEFAULT_TIMEZONE = "Asia/Manila"
CONFIG_RELATIVE_PATH = Path("config") / "extraction.toml"
REPO_MARKER = "pyproject.toml"


class ConfigError(RuntimeError):
    """Raised when required configuration is missing or malformed."""


def find_repo_root(start: Path | None = None) -> Path:
    """Walk upwards for the project marker so the package works installed or in place."""
    for base in (start, PACKAGE_ROOT, Path.cwd()):
        if base is None:
            continue
        for directory in (base, *base.parents):
            if (directory / REPO_MARKER).is_file():
                return directory
    parents = PACKAGE_ROOT.parents
    return parents[2] if len(parents) > 2 else PACKAGE_ROOT


REPO_ROOT = find_repo_root()


def _parse_env_file(path: Path) -> None:
    text = path.read_text(encoding="utf-8-sig")
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def load_env() -> None:
    load_env_from(REPO_ROOT / ".env")


def load_env_from(path: Path) -> None:
    if path.is_file():
        _parse_env_file(path)


def load_toml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        return {}
    try:
        return tomllib.loads(path.read_text(encoding="utf-8"))
    except tomllib.TOMLDecodeError as exc:
        raise ConfigError(f"{path} is not valid TOML: {exc}") from exc


def _section(document: dict[str, Any], name: str) -> dict[str, Any]:
    value = document.get(name, {})
    return value if isinstance(value, dict) else {}


@dataclass(frozen=True)
class Settings:
    supabase_url: str
    service_role_key: str
    run_started_at: datetime
    run_id: str
    timezone: ZoneInfo
    page_size: int
    retry_limit: int
    retry_backoff_seconds: float
    request_timeout_seconds: float
    operational_retention_days: int
    audit_retention_days: int
    raw_root: Path
    config_path: Path

    @property
    def rest_base(self) -> str:
        return f"{self.supabase_url.rstrip('/')}/rest/v1"

    @property
    def run_date(self) -> date:
        return self.run_started_at.date()

    def now(self) -> datetime:
        return datetime.now(self.timezone)

    def table_directory(self, table_name: str) -> Path:
        return self.raw_root / table_name / self.run_date.isoformat()

    def metadata_path(self) -> Path:
        return self.raw_root / "_metadata" / f"{self.run_id}.json"

    def validation_path(self) -> Path:
        return self.raw_root / "_validation" / f"{self.run_id}.json"

    def handover_path(self) -> Path:
        return self.raw_root / "_handover" / f"{self.run_id}.json"


def _resolve_run_id(raw_root: Path, moment: datetime) -> str:
    directory = raw_root / "_metadata"
    base = moment.strftime("spartment-extract-%Y-%m-%d-%H%M")
    if not (directory / f"{base}.json").exists():
        return base
    attempt = 2
    while (directory / f"{base}-{attempt}.json").exists():
        attempt += 1
    return f"{base}-{attempt}"


def load_retention_days(config_path: Path | None = None) -> tuple[int, int]:
    """Read retention periods without requiring credentials.

    Retention must remain runnable on a host that has no database credentials,
    so this deliberately does not load the environment.
    """
    path = config_path or (REPO_ROOT / CONFIG_RELATIVE_PATH)
    retention = _section(load_toml(path), "retention")
    operational = int(retention.get("operational_days", 90))
    audit = int(retention.get("audit_days", 365))
    if operational < 1 or audit < 1:
        raise ConfigError("retention periods must be at least one day")
    if audit < operational:
        raise ConfigError("audit retention must not be shorter than operational retention")
    return operational, audit


def load_settings(
    run_date: date | None = None,
    page_size: int | None = None,
    retry_limit: int | None = None,
    config_path: Path | None = None,
) -> Settings:
    load_env()

    path = config_path or (REPO_ROOT / CONFIG_RELATIVE_PATH)
    document = load_toml(path)
    extraction = _section(document, "extraction")
    retention = _section(document, "retention")

    timezone = ZoneInfo(str(extraction.get("timezone", DEFAULT_TIMEZONE)))
    now = datetime.now(timezone)
    if run_date is not None:
        now = datetime.combine(run_date, now.timetz())

    supabase_url = os.environ.get("SUPABASE_URL", "").strip()
    service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY", "").strip()

    missing = [
        name
        for name, value in (
            ("SUPABASE_URL", supabase_url),
            ("SUPABASE_SERVICE_ROLE_KEY", service_role_key),
        )
        if not value
    ]
    if missing:
        raise ConfigError(
            f"missing required environment variables: {', '.join(missing)}. "
            "Set them in a .env file at the repository root. "
            "See .env.example for the expected names."
        )

    resolved_page_size = int(page_size or extraction.get("page_size", 1000))
    resolved_retry_limit = int(retry_limit or extraction.get("retry_limit", 3))
    if resolved_page_size < 1:
        raise ConfigError("page size must be at least 1")
    if resolved_retry_limit < 1:
        raise ConfigError("retry limit must be at least 1")

    raw_root = REPO_ROOT / "rawdata"
    return Settings(
        supabase_url=supabase_url,
        service_role_key=service_role_key,
        run_started_at=now,
        run_id=_resolve_run_id(raw_root, now),
        timezone=timezone,
        page_size=resolved_page_size,
        retry_limit=resolved_retry_limit,
        retry_backoff_seconds=float(extraction.get("retry_backoff_seconds", 2.0)),
        request_timeout_seconds=float(extraction.get("request_timeout_seconds", 60.0)),
        operational_retention_days=int(retention.get("operational_days", 90)),
        audit_retention_days=int(retention.get("audit_days", 365)),
        raw_root=raw_root,
        config_path=path,
    )
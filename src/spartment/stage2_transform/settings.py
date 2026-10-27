"""Stage 2 settings resolved without database credentials."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from spartment.common.config import REPO_ROOT
from spartment.stage2_transform.contract import PHT
from spartment.stage2_transform.models import TransformConfigError, TransformSettings


def _run_id_exists(settings: TransformSettings, run_id: str) -> bool:
    return any(settings.log_root.glob(f"*/{run_id}.json"))


def _resolve_run_id(settings: TransformSettings, moment: datetime) -> str:
    base = moment.strftime("spartment-transform-%Y-%m-%d-%H%M")
    if not _run_id_exists(settings, base):
        return base
    attempt = 2
    while _run_id_exists(settings, f"{base}-{attempt}"):
        attempt += 1
    return f"{base}-{attempt}"


def build_settings(
    raw_root: Path | None = None,
    output_root: Path | None = None,
    log_root: Path | None = None,
    quarantine_root: Path | None = None,
    reference_date: date | None = None,
    run_id: str | None = None,
    allow_manual_review: bool = False,
    dry_run: bool = False,
) -> tuple[TransformSettings, str]:
    now = datetime.now(PHT)
    resolved_reference = reference_date or now.date()
    settings = TransformSettings(
        raw_root=(raw_root or REPO_ROOT / "rawdata").resolve(),
        output_root=(output_root or REPO_ROOT / "transformed").resolve(),
        log_root=(log_root or REPO_ROOT / "logs" / "spartment-transformation").resolve(),
        quarantine_root=(quarantine_root or REPO_ROOT / "quarantine" / "transformation").resolve(),
        reference_date=resolved_reference,
        allow_manual_review=allow_manual_review,
        dry_run=dry_run,
    )
    if not settings.raw_root.is_dir():
        raise TransformConfigError(f"raw data root does not exist: {settings.raw_root}")
    resolved_run_id = run_id or _resolve_run_id(settings, now)
    if not resolved_run_id.strip():
        raise TransformConfigError("run_id must not be empty")
    return settings, resolved_run_id

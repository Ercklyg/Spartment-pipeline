"""Stage 2 log and quarantine retention, transformation.md section 9.

Per-table transformation logs are operational records. Once outside the
operational window they are compressed into the archive tree before being
removed from the online log tree. Run summaries and rejected-record quarantine
files are audit records and are kept for the longer audit window.

Dry runs are the default. Quarantine deletion is separately opt-in so the
financial-record approval rule cannot be bypassed by an automatic retention
task.
"""

from __future__ import annotations

import argparse
import gzip
import shutil
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from spartment.common.config import (
    REPO_ROOT,
    ConfigError,
    load_retention_days,
)
from spartment.common.io import write_json
from spartment.stage2_transform.contract import OUTPUT_TABLES

DEFAULT_OPERATIONAL_DAYS = 90
DEFAULT_AUDIT_DAYS = 365
RETENTION_LOG_DIRECTORY = "_retention"
ARCHIVE_DIRECTORY = "_archive"
TABLE_LOG_CATEGORY = "table_log"
SUMMARY_CATEGORY = "run_summary"
QUARANTINE_CATEGORY = "quarantine"
LOG_CATEGORIES = frozenset({TABLE_LOG_CATEGORY, SUMMARY_CATEGORY})

_DEFAULT_LOG_ROOT = REPO_ROOT / "logs" / "spartment-transformation"
_DEFAULT_QUARANTINE_ROOT = REPO_ROOT / "quarantine" / "transformation"


@dataclass(frozen=True)
class Candidate:
    path: Path
    category: str
    retention_days: int
    size_bytes: int
    modified_at: datetime


@dataclass
class RetentionResult:
    operational_days: int
    audit_days: int
    applied: bool
    candidates: list[Candidate]
    archived: list[str]
    deleted: list[str]


def _threshold(days: int, now: datetime) -> datetime:
    return now - timedelta(days=days)


def _is_table_log(path: Path) -> bool:
    return any(path.stem.endswith(f"_{table}") for table in OUTPUT_TABLES)


def _candidate(path: Path, category: str, days: int, now: datetime) -> Candidate | None:
    try:
        stat = path.stat()
    except FileNotFoundError:
        return None
    modified = datetime.fromtimestamp(stat.st_mtime, tz=ZoneInfo("UTC"))
    if modified >= _threshold(days, now):
        return None
    return Candidate(
        path=path,
        category=category,
        retention_days=days,
        size_bytes=stat.st_size,
        modified_at=modified,
    )


def collect(
    log_root: Path,
    quarantine_root: Path,
    operational_days: int,
    audit_days: int,
    now: datetime,
) -> list[Candidate]:
    candidates: list[Candidate] = []

    if log_root.is_dir():
        for path in sorted(log_root.rglob("*.json")):
            relative = path.relative_to(log_root)
            if relative.parts and relative.parts[0] in {
                RETENTION_LOG_DIRECTORY,
                ARCHIVE_DIRECTORY,
            }:
                continue
            if not path.is_file():
                continue
            if _is_table_log(path):
                category, days = TABLE_LOG_CATEGORY, operational_days
            else:
                category, days = SUMMARY_CATEGORY, audit_days
            entry = _candidate(path, category, days, now)
            if entry is not None:
                candidates.append(entry)

    if quarantine_root.is_dir():
        for path in sorted(quarantine_root.rglob("*.csv")):
            if not path.is_file():
                continue
            entry = _candidate(path, QUARANTINE_CATEGORY, audit_days, now)
            if entry is not None:
                candidates.append(entry)

    return candidates


def _archive_path(archive_root: Path, log_root: Path, candidate: Candidate) -> Path:
    relative = candidate.path.relative_to(log_root)
    return archive_root / relative.parent / f"{relative.name}.gz"


def _archive_candidate(candidate: Candidate, archive_path: Path) -> None:
    archive_path.parent.mkdir(parents=True, exist_ok=True)
    with candidate.path.open("rb") as source, gzip.open(archive_path, "wb") as target:
        shutil.copyfileobj(source, target)


def _remove_empty_directories(root: Path, preserved: set[str]) -> None:
    if not root.is_dir():
        return
    for directory in sorted(root.iterdir()):
        if not directory.is_dir() or directory.name in preserved:
            continue
        if not any(directory.iterdir()):
            directory.rmdir()


def _write_deletion_log(
    log_root: Path,
    now: datetime,
    operational_days: int,
    audit_days: int,
    archived: list[tuple[Candidate, Path]],
    deleted: list[Candidate],
) -> Path:
    path = (
        log_root
        / RETENTION_LOG_DIRECTORY
        / f"{now.strftime('%Y%m%dT%H%M%S%z')}.json"
    )
    payload: dict[str, Any] = {
        "stage": "transformation",
        "deleted_at": now.isoformat(),
        "operational_days": operational_days,
        "audit_days": audit_days,
        "archived": [
            {
                "path": str(candidate.path),
                "archive_path": str(archive_path),
                "category": candidate.category,
                "retention_days": candidate.retention_days,
                "size_bytes": candidate.size_bytes,
                "modified_at": candidate.modified_at.isoformat(),
            }
            for candidate, archive_path in archived
        ],
        "deleted": [
            {
                "path": str(candidate.path),
                "category": candidate.category,
                "retention_days": candidate.retention_days,
                "size_bytes": candidate.size_bytes,
                "modified_at": candidate.modified_at.isoformat(),
            }
            for candidate in deleted
        ],
    }
    write_json(path, payload)
    return path


def apply_retention(
    log_root: Path,
    quarantine_root: Path,
    operational_days: int = DEFAULT_OPERATIONAL_DAYS,
    audit_days: int = DEFAULT_AUDIT_DAYS,
    apply: bool = False,
    now: datetime | None = None,
    timezone: ZoneInfo | None = None,
    archive_root: Path | None = None,
    include_summaries: bool = True,
    include_quarantine: bool = False,
) -> RetentionResult:
    tz = timezone or ZoneInfo("Asia/Manila")
    moment = now or datetime.now(tz)
    archive_dir = archive_root or (log_root / ARCHIVE_DIRECTORY)
    candidates = [
        candidate
        for candidate in collect(
            log_root,
            quarantine_root,
            operational_days,
            audit_days,
            moment,
        )
        if candidate.category == TABLE_LOG_CATEGORY
        or (candidate.category == SUMMARY_CATEGORY and include_summaries)
        or (candidate.category == QUARANTINE_CATEGORY and include_quarantine)
    ]

    archived: list[tuple[Candidate, Path]] = []
    deleted: list[Candidate] = []
    if apply:
        for candidate in candidates:
            if candidate.category in LOG_CATEGORIES:
                archive_path = _archive_path(archive_dir, log_root, candidate)
                _archive_candidate(candidate, archive_path)
                archived.append((candidate, archive_path))
            candidate.path.unlink(missing_ok=True)
            deleted.append(candidate)
        _remove_empty_directories(
            log_root,
            {RETENTION_LOG_DIRECTORY, ARCHIVE_DIRECTORY},
        )
        _remove_empty_directories(quarantine_root, set())
        if deleted:
            _write_deletion_log(
                log_root,
                moment,
                operational_days,
                audit_days,
                archived,
                deleted,
            )

    return RetentionResult(
        operational_days=operational_days,
        audit_days=audit_days,
        applied=apply,
        candidates=candidates,
        archived=[str(archive_path) for _, archive_path in archived],
        deleted=[str(candidate.path) for candidate in deleted],
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="spartment-transform-retain",
        description="Archive/prune Stage 2 logs and quarantine per the retention policy.",
    )
    parser.add_argument("--log-root", type=Path, default=_DEFAULT_LOG_ROOT)
    parser.add_argument(
        "--quarantine-root",
        type=Path,
        default=_DEFAULT_QUARANTINE_ROOT,
    )
    parser.add_argument(
        "--archive-root",
        type=Path,
        default=None,
        help="compressed log archive root (default: <log-root>/_archive)",
    )
    parser.add_argument(
        "--operational-days",
        type=int,
        default=None,
        help="override config/extraction.toml retention.operational_days",
    )
    parser.add_argument(
        "--audit-days",
        type=int,
        default=None,
        help="override config/extraction.toml retention.audit_days",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="perform archival/deletions. Without this flag nothing is changed.",
    )
    parser.add_argument(
        "--include-quarantine",
        action="store_true",
        help=(
            "include quarantine files in the plan. Required to delete them after "
            "audit approval, including Super Admin approval for financial records."
        ),
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        configured_operational, configured_audit = load_retention_days()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    operational_days = (
        configured_operational
        if args.operational_days is None
        else args.operational_days
    )
    audit_days = configured_audit if args.audit_days is None else args.audit_days
    if operational_days < 1 or audit_days < 1:
        print("retention periods must be at least one day", file=sys.stderr)
        return 2
    if audit_days < operational_days:
        print("audit retention must not be shorter than operational retention", file=sys.stderr)
        return 2
    if not args.log_root.is_dir() and not args.quarantine_root.is_dir():
        print(
            "neither the transformation log root nor the quarantine root exists",
            file=sys.stderr,
        )
        return 2

    result = apply_retention(
        args.log_root,
        args.quarantine_root,
        operational_days=operational_days,
        audit_days=audit_days,
        apply=args.apply,
        archive_root=args.archive_root,
        include_quarantine=args.include_quarantine,
    )

    print(f"mode            {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"operational     {result.operational_days} days")
    print(f"audit           {result.audit_days} days")
    print(f"candidates      {len(result.candidates)}")
    print(f"total size      {sum(c.size_bytes for c in result.candidates)} bytes")
    print()
    for candidate in result.candidates:
        if candidate.category in LOG_CATEGORIES:
            verb = "archived" if args.apply else "would archive"
        else:
            verb = "deleted" if args.apply else "would delete"
        print(
            f"  {verb:<13}  {candidate.category:<14}{candidate.modified_at.date()}  "
            f"{candidate.size_bytes:>10}  {candidate.path}"
        )
    if not result.candidates:
        print("  nothing is outside the retention window")
    if not args.apply and result.candidates:
        print("\nre-run with --apply to perform the archival/deletions")
    if args.apply and result.archived:
        print(f"\narchived {len(result.archived)} log file(s) under the archive root")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

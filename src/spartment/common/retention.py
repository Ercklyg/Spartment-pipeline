"""Log and raw dataset retention, extraction.md section 5.

Operational run metadata is pruned after the operational retention period.
Handover and validation records are treated as audit summaries and are kept
for the longer audit retention period. Deletion is never performed unless the
apply flag is supplied, and nothing inside its retention window is removed.
"""

from __future__ import annotations

import argparse
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
    load_env,
    load_retention_days,
)

DEFAULT_OPERATIONAL_DAYS = 90
DEFAULT_AUDIT_DAYS = 365
AUDIT_DIRECTORIES = ("_metadata", "_validation", "_handover")


@dataclass
class Candidate:
    path: Path
    category: str
    size_bytes: int
    modified_at: datetime


@dataclass
class RetentionResult:
    operational_days: int
    audit_days: int
    applied: bool
    candidates: list[Candidate]
    deleted: list[str]
    retained: list[str]


def _threshold(days: int, now: datetime) -> datetime:
    return now - timedelta(days=days)


def collect(
    raw_root: Path,
    operational_days: int,
    audit_days: int,
    now: datetime,
) -> tuple[list[Candidate], list[Candidate]]:
    operational: list[Candidate] = []
    audit: list[Candidate] = []

    for directory in sorted(p for p in raw_root.glob("*") if p.is_dir()):
        is_audit = directory.name in AUDIT_DIRECTORIES
        days = audit_days if is_audit else operational_days
        limit = _threshold(days, now)
        for file in sorted(p for p in directory.rglob("*") if p.is_file()):
            modified = datetime.fromtimestamp(file.stat().st_mtime, tz=ZoneInfo("UTC"))
            if modified >= limit:
                continue
            entry = Candidate(
                path=file,
                category=directory.name,
                size_bytes=file.stat().st_size,
                modified_at=modified,
            )
            (audit if is_audit else operational).append(entry)

    return operational, audit


def apply_retention(
    raw_root: Path,
    operational_days: int = DEFAULT_OPERATIONAL_DAYS,
    audit_days: int = DEFAULT_AUDIT_DAYS,
    apply: bool = False,
    timezone: ZoneInfo | None = None,
) -> RetentionResult:
    load_env()
    tz = timezone or ZoneInfo("Asia/Manila")
    now = datetime.now(tz)

    operational, audit = collect(raw_root, operational_days, audit_days, now)
    candidates = operational + audit

    deleted: list[str] = []
    retained: list[str] = []
    for candidate in candidates:
        days = audit_days if candidate.category in AUDIT_DIRECTORIES else operational_days
        if apply:
            candidate.path.unlink(missing_ok=True)
            deleted.append(candidate.path.name)
        else:
            retained.append(candidate.path.name)

    for directory in sorted(p for p in raw_root.glob("*") if p.is_dir()):
        for empty in sorted(p for p in directory.rglob("*") if p.is_dir() and not any(p.iterdir())):
            if directory.name not in AUDIT_DIRECTORIES:
                empty.rmdir()
        if directory.name not in AUDIT_DIRECTORIES and not any(directory.iterdir()):
            directory.rmdir()

    return RetentionResult(
        operational_days=operational_days,
        audit_days=audit_days,
        applied=apply,
        candidates=candidates,
        deleted=deleted,
        retained=retained,
    )


def _parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="spartment-retain",
        description="Prune extraction logs and raw datasets per the retention policy.",
    )
    parser.add_argument("--raw-root", type=Path, default=REPO_ROOT / "rawdata")
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
        help="perform the deletions. Without this flag nothing is removed.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(argv)

    try:
        configured_operational, configured_audit = load_retention_days()
    except ConfigError as exc:
        print(f"configuration error: {exc}", file=sys.stderr)
        return 2

    operational_days = args.operational_days or configured_operational
    audit_days = args.audit_days or configured_audit

    if operational_days < 1 or audit_days < 1:
        print("retention periods must be at least one day", file=sys.stderr)
        return 2
    if audit_days < operational_days:
        print("audit retention must not be shorter than operational retention", file=sys.stderr)
        return 2
    if not args.raw_root.is_dir():
        print(f"raw root does not exist: {args.raw_root}", file=sys.stderr)
        return 2

    result = apply_retention(
        args.raw_root,
        operational_days=operational_days,
        audit_days=audit_days,
        apply=args.apply,
    )

    verb = "deleted" if args.apply else "would delete"
    total_bytes: Any = sum(c.size_bytes for c in result.candidates)
    print(f"mode            {'APPLY' if args.apply else 'DRY RUN'}")
    print(f"operational     {result.operational_days} days")
    print(f"audit           {result.audit_days} days")
    print(f"candidates      {len(result.candidates)}")
    print(f"total size      {total_bytes} bytes")
    print()
    for candidate in result.candidates:
        print(
            f"  {verb}  {candidate.category:<14}{candidate.modified_at.date()}  "
            f"{candidate.size_bytes:>10}  {candidate.path}"
        )
    if not result.candidates:
        print("  nothing is outside the retention window")
    if not args.apply and result.candidates:
        print("\nre-run with --apply to perform the deletions")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
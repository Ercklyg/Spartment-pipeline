"""Atomic CSV output and checksum helpers."""

from __future__ import annotations

import csv
import hashlib
import json
import os
from pathlib import Path
from typing import Any

from spartment.stage2_transform.contract import NATURAL_KEY, OUTPUT_COLUMNS
from spartment.stage2_transform.models import QuarantineRecord
from spartment.stage2_transform.standardize import mask_phone


def sort_rows(table: str, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    key = NATURAL_KEY[table]
    return sorted(rows, key=lambda row: str(row.get(key, "")))


def atomic_write_csv(
    path: Path,
    columns: tuple[str, ...],
    rows: list[dict[str, Any]],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=list(columns),
            extrasaction="ignore",
            lineterminator="\n",
        )
        writer.writeheader()
        for row in rows:
            writer.writerow({column: "" if row.get(column) is None else row.get(column) for column in columns})
    os.replace(temporary, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def content_checksum(table: str, rows: list[dict[str, Any]]) -> str:
    columns = [
        column
        for column in OUTPUT_COLUMNS[table]
        if column != "transformation_run_id"
    ]
    digest = hashlib.sha256()
    for row in rows:
        digest.update("\x1f".join(columns).encode("utf-8"))
        digest.update(b"\x1e")
        for column in columns:
            value = row.get(column)
            digest.update(("" if value is None else str(value)).encode("utf-8"))
            digest.update(b"\x1f")
        digest.update(b"\x1d")
    return digest.hexdigest()


def _masked_values(record: QuarantineRecord) -> dict[str, Any]:
    values = dict(record.source_values)
    for field in record.mask_fields:
        if field in values:
            values[field] = mask_phone(values[field])
    return values


def write_quarantine(
    path: Path,
    run_id: str,
    parent_run_id: str,
    records: list[QuarantineRecord],
) -> Path | None:
    if not records:
        if path.exists():
            path.unlink()
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    columns = (
        "run_id",
        "parent_run_id",
        "table_name",
        "source_table",
        "rule_id",
        "rejection_reason",
        "source_values",
    )
    with temporary.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "run_id": run_id,
                    "parent_run_id": parent_run_id,
                    "table_name": record.output_table,
                    "source_table": record.source_table,
                    "rule_id": record.rule_id,
                    "rejection_reason": record.rejection_reason,
                    "source_values": json.dumps(
                        _masked_values(record), ensure_ascii=False, default=str
                    ),
                }
            )
    os.replace(temporary, path)
    return path

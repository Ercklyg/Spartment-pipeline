"""Streams table rows from PostgREST into CSV files under rawdata/."""

from __future__ import annotations

import csv
import json
import os
import time
from pathlib import Path
from typing import Any

from spartment.common.config import Settings
from spartment.common.models import STATUS_FAILED, STATUS_SUCCESS, TableRun
from spartment.stage1_extract.guard import TablePlan
from spartment.stage1_extract.source import SupabaseError


def _cell(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (dict, list)):
        return json.dumps(value, separators=(",", ":"), ensure_ascii=False)
    return str(value)


def _target(settings: Settings, table_name: str) -> Path:
    return settings.table_directory(table_name) / f"{table_name}.csv"


def quarantined_run(settings: Settings, plan: TablePlan) -> TableRun:
    now = settings.now().isoformat()
    return TableRun(
        table_name=plan.spec.name,
        extraction_started_at=now,
        extraction_ended_at=now,
        extraction_status=STATUS_FAILED,
        records_read=0,
        records_extracted=0,
        records_read_total=None,
        records_rejected=0,
        extraction_window_start=None,
        extraction_window_end=now,
        validation_result="FAIL",
        error_message=plan.reason,
        error_code="SCHEMA_DRIFT",
        extraction_duration_ms=0,
        output_path=None,
    )


def extract_table(
    client: Any,
    plan: TablePlan,
    settings: Settings,
    max_rows: int | None = None,
) -> TableRun:
    target = _target(settings, plan.spec.name)
    temporary = target.with_name(target.name + ".part")
    started_at = settings.now()
    started_perf = time.perf_counter()
    extracted = 0
    source_total: int | None = None
    status = STATUS_SUCCESS
    error_message: str | None = None
    error_code: str | None = None

    try:
        source_total = client.count_total(plan.spec.name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with temporary.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=list(plan.selected),
                quoting=csv.QUOTE_ALL,
                extrasaction="ignore",
            )
            writer.writeheader()
            for row in client.paginate(plan.spec.name, plan.selected):
                if max_rows is not None and extracted >= max_rows:
                    break
                writer.writerow({name: _cell(row.get(name)) for name in plan.selected})
                extracted += 1
        os.replace(temporary, target)
        output_path: str | None = str(target.relative_to(settings.raw_root))
    except SupabaseError as exc:
        temporary.unlink(missing_ok=True)
        status = STATUS_FAILED
        error_message = str(exc)
        error_code = exc.code or (f"HTTP_{exc.status}" if exc.status else "UNKNOWN")
        output_path = None
    except OSError as exc:
        temporary.unlink(missing_ok=True)
        status = STATUS_FAILED
        error_message = f"filesystem error: {exc}"
        error_code = "IO_ERROR"
        output_path = None

    ended_at = settings.now()
    duration_ms = int((time.perf_counter() - started_perf) * 1000)

    return TableRun(
        table_name=plan.spec.name,
        extraction_started_at=started_at.isoformat(),
        extraction_ended_at=ended_at.isoformat(),
        extraction_status=status,
        records_read=extracted,
        records_extracted=extracted,
        records_read_total=source_total,
        records_rejected=0,
        extraction_window_start=None,
        extraction_window_end=ended_at.isoformat(),
        validation_result="NOT_EVALUATED" if status == STATUS_SUCCESS else "FAIL",
        error_message=error_message,
        error_code=error_code,
        extraction_duration_ms=duration_ms,
        output_path=output_path,
    )
"""Read and profile the Stage 1 handover without contacting the source."""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from spartment.common.models import (
    DECISION_APPROVED,
    DECISION_MANUAL_REVIEW,
    DECISION_REJECTED,
)
from spartment.stage2_transform.contract import (
    AREA_FIELDS,
    BILLING_PERIOD_PATTERN,
    BOOL_FIELDS,
    CODE_FIELDS,
    CODE_MAPS,
    DATE_FIELDS,
    EMAIL_FIELDS,
    INPUT_COLUMNS,
    INPUT_OPTIONAL,
    INPUT_REQUIRED,
    JSON_FIELDS,
    MANIFEST_DATA_TYPES,
    MONEY_FIELDS,
    SOURCE_TABLES,
)
from spartment.stage2_transform.models import (
    Batch,
    TransformContext,
    TransformInputError,
    TransformRetryableError,
    TransformSchemaError,
    TransformSettings,
)
from spartment.stage2_transform.standardize import money

CONTROL_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")
SPACE_RE = re.compile(r"\s+")
PHONE_RE = re.compile(r"^[0-9+()\-\s]+$")
WHITESPACE_ISSUE_RE = re.compile(r"\s{2,}|[^\S ]")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise TransformRetryableError(f"cannot read {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise TransformInputError(f"{path} must contain a JSON object")
    return payload


def _select_handover(settings: TransformSettings, parent_run_id: str | None) -> Path:
    if parent_run_id:
        path = settings.handover_dir / f"{parent_run_id}.json"
        if not path.is_file():
            raise TransformInputError(f"parent handover not found: {path}")
        return path
    files = sorted(
        settings.handover_dir.glob("*.json"),
        key=lambda item: item.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise TransformInputError(f"no Stage 1 handover found in {settings.handover_dir}")
    for path in files:
        payload = _read_json(path)
        if str(payload.get("processing_period", "")) == settings.reference_date.isoformat():
            return path
    return files[0]


def _dataset_path(
    settings: TransformSettings,
    raw_datasets: dict[str, Any],
    table: str,
    run_date: str,
) -> Path:
    configured = raw_datasets.get(table)
    if configured:
        candidate = Path(str(configured))
        return candidate if candidate.is_absolute() else settings.raw_root / candidate
    return settings.raw_root / table / run_date / f"{table}.csv"


def _read_csv(path: Path, table: str) -> tuple[tuple[str, ...], list[dict[str, str]]]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            headers = tuple(reader.fieldnames or ())
            rows = [dict(row) for row in reader]
    except (OSError, UnicodeError, csv.Error) as exc:
        raise TransformRetryableError(
            f"cannot read staged {table} dataset {path}: {exc}"
        ) from exc
    if not headers:
        raise TransformSchemaError(f"staged {table} dataset has no header: {path}")
    return headers, rows


def _records_extracted(metadata: dict[str, Any]) -> dict[str, int]:
    result: dict[str, int] = {}
    tables = metadata.get("tables")
    if not isinstance(tables, list):
        return result
    for item in tables:
        if not isinstance(item, dict):
            continue
        name = item.get("table_name")
        if not name:
            continue
        value = item.get("records_extracted", item.get("records_read", 0))
        try:
            result[str(name)] = int(value)
        except (TypeError, ValueError):
            result[str(name)] = 0
    return result


def _batch_window(metadata: dict[str, Any], run_date: str) -> tuple[str, str]:
    starts: list[str] = []
    ends: list[str] = []
    tables = metadata.get("tables")
    if isinstance(tables, list):
        for item in tables:
            if not isinstance(item, dict):
                continue
            start = item.get("extraction_started_at", item.get("extraction_window_start"))
            end = item.get("extraction_ended_at", item.get("extraction_window_end"))
            if start:
                starts.append(str(start))
            if end:
                ends.append(str(end))
    return (
        min(starts) if starts else f"{run_date}T01:00:00+08:00",
        max(ends) if ends else f"{run_date}T02:00:00+08:00",
    )


def _blank(value: Any) -> bool:
    return value is None or str(value).strip() == ""


def _normalise_text(value: Any) -> str:
    text = SPACE_RE.sub(" ", CONTROL_RE.sub("", str(value))).strip()
    return text


def _dec(value: Any) -> Decimal | None:
    if _blank(value):
        return None
    try:
        return Decimal(str(value).strip().replace(",", ""))
    except (InvalidOperation, ValueError):
        return None


def _date(value: Any) -> date | None:
    if _blank(value):
        return None
    try:
        return date.fromisoformat(str(value).strip()[:10])
    except ValueError:
        return None


def _json_object(value: Any) -> bool:
    if _blank(value):
        return True
    try:
        return isinstance(json.loads(str(value)), dict)
    except (json.JSONDecodeError, TypeError, ValueError):
        return False


def _uuids(rows: list[dict[str, str]], column: str) -> set[str]:
    return {
        str(row.get(column, "")).strip().lower()
        for row in rows
        if not _blank(row.get(column))
    }


def _count_code_variants(rows: list[dict[str, str]], table: str, column: str) -> int:
    maps = dict(CODE_FIELDS.get(table, ()))
    key = maps.get(column)
    if not key:
        return 0
    canonical = set(CODE_MAPS[key].values())
    return sum(
        1
        for row in rows
        if _normalise_text(row.get(column, "")) not in canonical
    )


def profile(
    batch: Batch,
    ctx: TransformContext,
) -> dict[str, int]:
    counts: Counter[str] = Counter()
    rows_by_table = batch.rows

    for table, rows in rows_by_table.items():
        text_columns = [
            column
            for column in INPUT_COLUMNS[table]
            if not (
                column in DATE_FIELDS.get(table, ())
                or column in TIMESTAMP_FIELDS_FOR(table)
                or column in MONEY_FIELDS.get(table, ())
                or column in AREA_FIELDS.get(table, ())
                or column in BOOL_FIELDS.get(table, ())
                or column in JSON_FIELDS.get(table, ())
            )
        ]
        counts[f"profile_{table}_p01_count"] = sum(
            1
            for row in rows
            if any(
                isinstance(row.get(column), str)
                and (
                    row[column] != row[column].strip()
                    or WHITESPACE_ISSUE_RE.search(row[column]) is not None
                    or CONTROL_RE.search(row[column]) is not None
                )
                for column in text_columns
            )
        )

    profiles = rows_by_table["profiles"]
    counts["profile_profiles_p02_count"] = sum(
        1 for row in profiles if _blank(row.get("phone"))
    )

    for table in ("tenant_assignments", "inquiries"):
        counts[f"profile_{table}_p03_count"] = sum(
            1
            for row in rows_by_table[table]
            if any(character.isupper() for character in str(row.get("email", "")))
        )

    counts["profile_tenant_assignments_p04_count"] = sum(
        1
        for row in rows_by_table["tenant_assignments"]
        if _blank(row.get("phone"))
        or not PHONE_RE.fullmatch(_normalise_text(row.get("phone", "")))
    )

    counts["profile_rooms_p05_count"] = sum(
        1
        for row in rows_by_table["rooms"]
        if (value := _dec(row.get("size_sqm"))) is not None
        and (value < Decimal("8") or value > Decimal("14"))
    )

    code_targets = (
        ("rooms", "status", "p06"),
        ("profiles", "role", "p07"),
        ("leases", "status", "p08"),
        ("tenant_assignments", "status", "p09"),
        ("bills", "status", "p10"),
        ("bills", "bill_type", "p11"),
    )
    for table, column, issue in code_targets:
        counts[f"profile_{table}_{issue}_count"] = _count_code_variants(
            rows_by_table[table], table, column
        )

    counts["profile_bills_p12_count"] = sum(
        1
        for row in rows_by_table["bills"]
        if any(
            (value := _dec(row.get(column))) is None or value < 0
            for column in ("rent_amount", "electricity_amount", "water_amount")
        )
    )
    counts["profile_bills_p13_count"] = sum(
        1
        for row in rows_by_table["bills"]
        if (period := _date(row.get("billing_period"))) is not None
        and (due := _date(row.get("due_date"))) is not None
        and due < period
    )
    counts["profile_leases_p14_count"] = sum(
        1
        for row in rows_by_table["leases"]
        if (start := _date(row.get("move_in_date"))) is not None
        and (end := _date(row.get("lease_end_date"))) is not None
        and end < start
    )
    counts["profile_tenant_assignments_p14_count"] = sum(
        1
        for row in rows_by_table["tenant_assignments"]
        if (start := _date(row.get("move_in"))) is not None
        and (end := _date(row.get("lease_end"))) is not None
        and end < start
    )
    counts["profile_payments_p15_count"] = sum(
        1
        for row in rows_by_table["payments"]
        if (value := _dec(row.get("amount"))) is None or value < 0
    )

    payment_keys = Counter(
        (
            str(row.get("bill_id", "")).strip().lower(),
            str(row.get("payment_date", "")).strip()[:10],
            str(money(_dec(row.get("amount")))),
        )
        for row in rows_by_table["payments"]
    )
    counts["profile_payments_p16_count"] = sum(value - 1 for value in payment_keys.values())
    counts["profile_payments_p17_count"] = sum(
        1
        for row in rows_by_table["payments"]
        if (value := _date(row.get("payment_date"))) is not None
        and value > batch.reference_date
    )

    ids = {
        table: _uuids(rows, "id")
        for table, rows in rows_by_table.items()
    }
    orphan_targets = (
        ("rooms", "building_id", "buildings"),
        ("leases", "tenant_id", "profiles"),
        ("leases", "room_id", "rooms"),
        ("tenant_assignments", "room_id", "rooms"),
        ("bills", "lease_id", "leases"),
        ("payments", "bill_id", "bills"),
        ("payment_corrections", "payment_id", "payments"),
        ("inquiries", "room_id", "rooms"),
        ("tenant_requests", "tenant_id", "profiles"),
        ("tenant_requests", "room_id", "rooms"),
        ("tenant_occupancy_events", "tenant_id", "profiles"),
        ("tenant_occupancy_events", "previous_lease_id", "leases"),
        ("tenant_occupancy_events", "new_lease_id", "leases"),
        ("tenant_occupancy_events", "performed_by", "profiles"),
        ("notifications", "tenant_id", "profiles"),
    )
    for child, column, parent in orphan_targets:
        counts[f"profile_{child}_p18_count"] += sum(
            1
            for row in rows_by_table[child]
            if not _blank(row.get(column))
            and str(row.get(column, "")).strip().lower() not in ids[parent]
        )

    counts["profile_profiles_p19_count"] = sum(
        1 for row in profiles if _blank(row.get("phone"))
    )
    counts["profile_tenant_assignments_p19_count"] = sum(
        1
        for row in rows_by_table["tenant_assignments"]
        if _blank(row.get("room_id")) or _blank(row.get("lease_end"))
    )
    counts["profile_tenant_requests_p19_count"] = sum(
        1
        for row in rows_by_table["tenant_requests"]
        if _blank(row.get("room_id"))
    )
    counts["profile_tenant_occupancy_events_p19_count"] = sum(
        1
        for row in rows_by_table["tenant_occupancy_events"]
        if _blank(row.get("new_lease_id"))
    )
    counts["profile_inquiries_p19_count"] = sum(
        1
        for row in rows_by_table["inquiries"]
        if _blank(row.get("room_id")) or _blank(row.get("phone"))
    )
    counts["profile_payments_p19_count"] = sum(
        1 for row in rows_by_table["payments"] if _blank(row.get("reference_note"))
    )
    counts["profile_payment_corrections_p19_count"] = sum(
        1
        for row in rows_by_table["payment_corrections"]
        if _blank(row.get("corrected_by"))
    )
    counts["profile_notifications_p19_count"] = sum(
        1 for row in rows_by_table["notifications"] if _blank(row.get("source_id"))
    )
    counts["profile_rooms_p19_count"] = sum(
        1 for row in rows_by_table["rooms"] if _blank(row.get("description"))
    )

    counts["profile_payment_corrections_p20_count"] = sum(
        1
        for row in rows_by_table["payment_corrections"]
        if not _json_object(row.get("previous_values"))
        or not _json_object(row.get("corrected_values"))
    )
    counts["profile_tenant_occupancy_events_p20_count"] = sum(
        1
        for row in rows_by_table["tenant_occupancy_events"]
        if not _json_object(row.get("details"))
    )

    counts["profile_inquiries_p21_count"] = _count_code_variants(
        rows_by_table["inquiries"], "inquiries", "status"
    )
    counts["profile_tenant_requests_p21_count"] = sum(
        1
        for row in rows_by_table["tenant_requests"]
        if any(
            _count_code_variants([row], "tenant_requests", column)
            for column in ("kind", "priority", "status")
        )
    )

    counts["profile_rooms_p22_count"] = sum(
        1
        for row in rows_by_table["rooms"]
        if not _blank(row.get("building_id"))
        and str(row.get("building_id", "")).strip().lower() not in ids["buildings"]
    )
    counts["profile_bills_p23_count"] = sum(
        1
        for row in rows_by_table["bills"]
        if not _blank(row.get("lease_id"))
        and str(row.get("lease_id", "")).strip().lower() not in ids["leases"]
    )

    ctx.profile_counts = dict(counts)
    return dict(counts)


def TIMESTAMP_FIELDS_FOR(table: str) -> tuple[str, ...]:
    from spartment.stage2_transform.contract import TIMESTAMP_FIELDS

    return TIMESTAMP_FIELDS.get(table, ())


def load_batch(settings: TransformSettings, parent_run_id: str | None = None) -> Batch:
    handover_path = _select_handover(settings, parent_run_id)
    handover = _read_json(handover_path)
    resolved_parent = str(handover.get("pipeline_run_id") or handover_path.stem)

    decision_block = handover.get("acceptance_decision")
    if not isinstance(decision_block, dict):
        raise TransformSchemaError(
            f"parent handover {handover_path} has no acceptance decision object"
        )
    decision = str(decision_block.get("decision", ""))
    if decision == DECISION_REJECTED:
        raise TransformInputError(f"parent run {resolved_parent} was REJECTED")
    if decision == DECISION_MANUAL_REVIEW and not settings.allow_manual_review:
        raise TransformInputError(
            f"parent run {resolved_parent} requires MANUAL REVIEW; "
            "rerun with --allow-manual-review to accept the parent"
        )
    if not decision:
        raise TransformSchemaError(f"parent handover {handover_path} has no acceptance decision")
    if decision not in {DECISION_APPROVED, DECISION_MANUAL_REVIEW, DECISION_REJECTED}:
        raise TransformSchemaError(
            f"parent handover {handover_path} has an unknown acceptance decision: {decision}"
        )

    run_date = str(handover.get("processing_period", "")).strip()
    if not run_date:
        raise TransformSchemaError(f"parent handover {handover_path} has no processing_period")
    try:
        date.fromisoformat(run_date)
    except ValueError as exc:
        raise TransformSchemaError(
            f"parent handover {handover_path} has an invalid processing_period: {run_date}"
        ) from exc

    metadata_path = settings.metadata_dir / f"{resolved_parent}.json"
    validation_path = settings.validation_dir / f"{resolved_parent}.json"
    if not metadata_path.is_file():
        raise TransformInputError(f"parent metadata record is missing: {metadata_path}")
    if not validation_path.is_file():
        raise TransformInputError(f"parent validation record is missing: {validation_path}")
    metadata = _read_json(metadata_path)
    validation = _read_json(validation_path)

    contract = handover.get("data_contract", {})
    manifest = contract.get("column_manifest", {}) if isinstance(contract, dict) else {}
    if not isinstance(manifest, dict) or not manifest:
        raise TransformSchemaError(
            f"parent handover {handover_path} has no data_contract.column_manifest"
        )

    raw_datasets = handover.get("raw_datasets", {})
    if not isinstance(raw_datasets, dict):
        raw_datasets = {}

    rows: dict[str, list[dict[str, str]]] = {}
    headers: dict[str, tuple[str, ...]] = {}
    records_read: dict[str, int] = {}
    extracted = _records_extracted(metadata)
    drift: dict[str, dict[str, list[str]]] = {}

    for table in SOURCE_TABLES:
        table_manifest = manifest.get(table)
        if not isinstance(table_manifest, dict) or not table_manifest:
            raise TransformSchemaError(
                f"parent handover has no column manifest for staged table {table}"
            )
        missing_from_manifest = [
            column for column in INPUT_REQUIRED[table] if column not in table_manifest
        ]
        if missing_from_manifest:
            raise TransformSchemaError(
                f"column manifest for {table} is missing required column(s): "
                f"{', '.join(missing_from_manifest)}"
            )
        for column in INPUT_COLUMNS[table]:
            if column not in table_manifest:
                continue
            definition = table_manifest[column]
            if isinstance(definition, str):
                data_type = definition
            elif isinstance(definition, dict):
                data_type = (
                    definition.get("data_type")
                    or definition.get("format")
                    or definition.get("type")
                )
            else:
                data_type = None
            if not isinstance(data_type, str) or not data_type.strip():
                raise TransformSchemaError(
                    f"column manifest for {table}.{column} declares no type"
                )
            normalized_type = data_type.strip().lower()
            if normalized_type not in MANIFEST_DATA_TYPES and not (
                normalized_type.startswith("public.")
                and normalized_type.replace(".", "").replace("_", "").isalnum()
            ):
                raise TransformSchemaError(
                    f"column manifest for {table}.{column} has unsupported "
                    f"type: {data_type}"
                )
        path = _dataset_path(settings, raw_datasets, table, run_date)
        if not path.is_file():
            raise TransformSchemaError(f"required staged dataset is missing: {path}")
        table_headers, table_rows = _read_csv(path, table)
        missing = [column for column in INPUT_REQUIRED[table] if column not in table_headers]
        if missing:
            raise TransformSchemaError(
                f"staged {table} is missing required column(s): {', '.join(missing)}"
            )
        extra = [column for column in table_headers if column not in INPUT_COLUMNS[table]]
        absent_optional = [
            column for column in INPUT_OPTIONAL.get(table, ()) if column not in table_headers
        ]
        if extra or absent_optional:
            drift[table] = {
                "input_column_unmapped": extra,
                "optional_column_absent": absent_optional,
            }
        rows[table] = table_rows
        headers[table] = table_headers
        records_read[table] = extracted.get(table, len(table_rows))

    start, end = _batch_window(metadata, run_date)
    batch = Batch(
        parent_run_id=resolved_parent,
        processing_period=run_date,
        handover=handover,
        metadata=metadata,
        validation=validation,
        rows=rows,
        headers=headers,
        manifest=manifest,
        records_read=records_read,
        parent_decision=decision,
        batch_window_start=start,
        batch_window_end=end,
        reference_date=settings.reference_date,
        column_drift=drift,
    )
    return batch


def manifest_columns(batch: Batch, table: str) -> dict[str, Any]:
    value = batch.manifest.get(table, {})
    return value if isinstance(value, dict) else {}


def manifest_has(batch: Batch, table: str, column: str) -> bool:
    return column in manifest_columns(batch, table)


__all__ = ["load_batch", "profile", "manifest_columns", "manifest_has"]

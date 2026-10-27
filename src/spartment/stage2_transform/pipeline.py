"""Stage 2 orchestration from handover to transformed handover."""

from __future__ import annotations

import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any, Callable

from spartment.common.config import REPO_ROOT
from spartment.common.io import write_json
from spartment.common.models import (
    DECISION_APPROVED,
    DECISION_MANUAL_REVIEW,
    RESULT_FAIL,
    RESULT_PASS,
    RESULT_WARNING,
    STATUS_FAILED,
    STATUS_SUCCESS,
    STATUS_WARNING,
)
from spartment.stage2_transform.acceptance import (
    decide,
    handover_payload,
    table_sources,
    table_status,
)
from spartment.stage2_transform.cleaning import (
    clean_table,
    deduplicate_assignments,
    deduplicate_payments,
    reject_orphans,
)
from spartment.stage2_transform.contract import (
    EXCLUSION_IDS,
    OUTPUT_COLUMNS,
    OUTPUT_TABLES,
    PHT,
    RULES_VERSION,
    SOURCE_TABLES,
)
from spartment.stage2_transform.dimensions import (
    apply_building_rollups,
    apply_tenant_rollups,
    build_dim_building,
    build_dim_date,
    build_dim_lease,
    build_dim_room,
    build_dim_tenant,
)
from spartment.stage2_transform.facts import (
    build_fact_assignment,
    build_fact_billing,
    build_fact_inquiry,
    build_fact_notification,
    build_fact_occupancy_event,
    build_fact_payment,
    build_fact_payment_correction,
    build_fact_tenant_request,
    enrich_billing,
)
from spartment.stage2_transform.models import (
    Batch,
    TransformContext,
    TransformError,
    TransformResult,
    TransformSchemaError,
    TransformSettings,
)
from spartment.stage2_transform.reader import load_batch, profile
from spartment.stage2_transform.standardize import bool_text
from spartment.stage2_transform.validation import OPTIONAL_COLUMNS, validate
from spartment.stage2_transform.writer import (
    atomic_write_csv,
    content_checksum,
    sha256_file,
    sort_rows,
    write_quarantine,
)


def _relative(path: Path) -> str:
    try:
        return path.relative_to(REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def _ensure_unique_source_ids(cleaned: dict[str, list[dict[str, Any]]]) -> None:
    for table, rows in cleaned.items():
        seen: set[str] = set()
        for row in rows:
            source_id = row.get("id")
            if source_id is None:
                continue
            if source_id in seen:
                raise TransformSchemaError(
                    f"staged {table} contains duplicate primary key {source_id}"
                )
            seen.add(source_id)


def _null_rates(table: str, rows: list[dict[str, Any]]) -> dict[str, float]:
    if not rows:
        return {}
    rates: dict[str, float] = {}
    for column in OPTIONAL_COLUMNS.get(table, ()):
        nulls = sum(1 for row in rows if row.get(column) is None or row.get(column) == "")
        rates[column] = round(nulls / len(rows), 6)
    return rates


def _timed(
    timings: dict[str, tuple[str, str, int]],
    table: str,
    builder: Callable[[], Any],
) -> Any:
    started = datetime.now(PHT)
    started_perf = time.perf_counter()
    result = builder()
    ended = datetime.now(PHT)
    duration_ms = int((time.perf_counter() - started_perf) * 1000)
    started_at = started.isoformat(timespec="seconds")
    if table in timings:
        started_at = timings[table][0]
        duration_ms += timings[table][2]
    timings[table] = (started_at, ended.isoformat(timespec="seconds"), duration_ms)
    return result


def transform(
    settings: TransformSettings,
    run_id: str,
    parent_run_id: str | None = None,
) -> TransformResult:
    started = datetime.now(PHT)
    started_perf = time.perf_counter()
    batch = load_batch(settings, parent_run_id)
    run_date = batch.processing_period

    ctx = TransformContext(
        run_id=run_id,
        parent_run_id=batch.parent_run_id,
        run_date=run_date,
        reference_date=settings.reference_date,
        rules_version=RULES_VERSION,
    )
    ctx.column_drift = batch.column_drift
    for exclusion in EXCLUSION_IDS:
        ctx.exclusions[exclusion] = 0
    profile(batch, ctx)

    cleaned: dict[str, list[dict[str, Any]]] = {}
    for table in SOURCE_TABLES:
        cleaned[table] = clean_table(table, batch.rows[table], ctx)
    cleaned["tenant_assignments"] = deduplicate_assignments(
        cleaned["tenant_assignments"], ctx
    )
    cleaned["payments"] = deduplicate_payments(cleaned["payments"], ctx)
    _ensure_unique_source_ids(cleaned)
    accepted = reject_orphans(cleaned, ctx)

    timings: dict[str, tuple[str, str, int]] = {}
    outputs: dict[str, list[dict[str, Any]]] = {}
    outputs["dim_date"] = _timed(
        timings, "dim_date", lambda: build_dim_date(accepted, ctx)
    )
    ctx.record_rule("B-02", "dim_date", len(outputs["dim_date"]), output_table="dim_date")
    outputs["dim_building"] = _timed(
        timings,
        "dim_building",
        lambda: build_dim_building(accepted, ctx, include_room_rollups=False),
    )
    building_keys = {
        row["building_id"]: row["building_key"] for row in outputs["dim_building"]
    }
    outputs["dim_room"] = _timed(
        timings,
        "dim_room",
        lambda: build_dim_room(accepted, ctx, building_keys),
    )
    _timed(
        timings,
        "dim_building",
        lambda: apply_building_rollups(outputs["dim_building"], outputs["dim_room"]),
    )
    room_keys = {row["room_id"]: row["room_key"] for row in outputs["dim_room"]}
    outputs["dim_tenant"] = _timed(
        timings,
        "dim_tenant",
        lambda: build_dim_tenant(accepted, ctx),
    )
    tenant_keys = {row["tenant_id"]: row["tenant_key"] for row in outputs["dim_tenant"]}
    outputs["dim_lease"] = _timed(
        timings,
        "dim_lease",
        lambda: build_dim_lease(accepted, ctx, tenant_keys, room_keys),
    )
    lease_outputs = {row["lease_id"]: row for row in outputs["dim_lease"]}
    lease_keys = {row["lease_id"]: row["lease_key"] for row in outputs["dim_lease"]}

    outputs["fact_assignment"] = _timed(
        timings,
        "fact_assignment",
        lambda: build_fact_assignment(accepted, ctx, tenant_keys, room_keys),
    )
    outputs["fact_billing"] = _timed(
        timings,
        "fact_billing",
        lambda: build_fact_billing(accepted, ctx, lease_outputs),
    )
    billing_outputs = {row["bill_id"]: row for row in outputs["fact_billing"]}
    outputs["fact_payment"] = _timed(
        timings,
        "fact_payment",
        lambda: build_fact_payment(accepted, ctx, billing_outputs, {}),
    )
    payment_keys = {row["payment_id"]: row["payment_key"] for row in outputs["fact_payment"]}
    outputs["fact_payment_correction"] = _timed(
        timings,
        "fact_payment_correction",
        lambda: build_fact_payment_correction(accepted, ctx, payment_keys),
    )
    correction_outputs = outputs["fact_payment_correction"]
    correction_counts = Counter(row["payment_id"] for row in correction_outputs)
    for row in outputs["fact_payment"]:
        count = correction_counts.get(row["payment_id"], 0)
        row["correction_count"] = count
        row["is_corrected"] = bool_text(count > 0)
    outputs["fact_inquiry"] = _timed(
        timings,
        "fact_inquiry",
        lambda: build_fact_inquiry(accepted, ctx, room_keys),
    )
    outputs["fact_tenant_request"] = _timed(
        timings,
        "fact_tenant_request",
        lambda: build_fact_tenant_request(accepted, ctx, tenant_keys, room_keys),
    )
    outputs["fact_occupancy_event"] = _timed(
        timings,
        "fact_occupancy_event",
        lambda: build_fact_occupancy_event(accepted, ctx, tenant_keys, lease_keys),
    )
    outputs["fact_notification"] = _timed(
        timings,
        "fact_notification",
        lambda: build_fact_notification(accepted, ctx, tenant_keys),
    )

    _timed(
        timings,
        "fact_billing",
        lambda: enrich_billing(outputs["fact_billing"], outputs["fact_payment"], ctx),
    )
    apply_tenant_rollups(outputs["dim_tenant"], outputs["fact_billing"], ctx)
    for table in OUTPUT_TABLES:
        outputs[table] = sort_rows(table, outputs[table])

    output_paths: dict[str, Path] = {}
    output_checksums: dict[str, str] = {}
    for table in OUTPUT_TABLES:
        path = settings.output_path(table, run_date)
        output_paths[table] = path
        if not settings.dry_run:
            try:
                atomic_write_csv(path, OUTPUT_COLUMNS[table], outputs[table])
            except OSError as exc:
                raise TransformError(f"cannot write {path}: {exc}") from exc
            output_checksums[table] = sha256_file(path)
    content_checksums = {
        table: content_checksum(table, outputs[table]) for table in OUTPUT_TABLES
    }

    report = validate(outputs, batch, ctx, settings)
    decision = decide(report)
    released = decision.decision == DECISION_APPROVED or (
        decision.decision == DECISION_MANUAL_REVIEW and settings.allow_manual_review
    )
    if decision.decision == DECISION_MANUAL_REVIEW and released:
        decision.promote = True
        decision.pipeline_action = (
            "Promote after explicit manual review release (--allow-manual-review)."
        )
    elif not released:
        decision.promote = False

    quarantine_paths: dict[str, Path] = {}
    if not settings.dry_run:
        for table in OUTPUT_TABLES:
            records = [
                record
                for source in SOURCE_TABLES
                for record in ctx.quarantine.get(source, [])
                if record.output_table == table
            ]
            path = settings.quarantine_path(run_date, table)
            written = write_quarantine(path, run_id, batch.parent_run_id, records)
            if written:
                quarantine_paths[table] = written

    statuses = table_status(report)
    table_logs: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        sources = table_sources(table)
        records_read = (
            len(outputs["dim_date"])
            if table == "dim_date"
            else sum(batch.records_read.get(source, 0) for source in sources)
        )
        output_records = [
            record
            for source in sources
            for record in ctx.quarantine.get(source, [])
            if record.output_table == table
        ]
        records_rejected = sum(
            1 for record in output_records if record.rule_id != "C-06"
        )
        duplicates = sum(1 for record in output_records if record.rule_id == "C-06")
        modified = sum(ctx.modified[source] for source in sources)
        rules: Counter[str] = Counter(ctx.rules_by_output.get(table, Counter()))
        if table == "dim_date":
            rules["B-02"] = len(outputs["dim_date"])
        exclusions: Counter[str] = Counter(ctx.exclusions_by_output.get(table, Counter()))
        relevant = [
            check
            for check in report.checks
            if table in check.target or any(source in check.target for source in sources)
        ]
        status = statuses[table]
        if status == RESULT_FAIL:
            transformation_status = STATUS_FAILED
        elif status == RESULT_WARNING:
            transformation_status = STATUS_WARNING
        else:
            transformation_status = STATUS_SUCCESS
        detail = "; ".join(
            f"{check.check_name}: {check.result}"
            for check in relevant
            if check.result != RESULT_PASS
        )
        if not detail:
            detail = status
        started_at, ended_at, duration_ms = timings.get(
            table,
            (
                started.isoformat(timespec="seconds"),
                datetime.now(PHT).isoformat(timespec="seconds"),
                int((time.perf_counter() - started_perf) * 1000),
            ),
        )
        log = {
            "run_id": run_id,
            "parent_run_id": batch.parent_run_id,
            "batch_id": f"{run_id}-b1",
            "batch_window_start": batch.batch_window_start,
            "batch_window_end": batch.batch_window_end,
            "reference_date": settings.reference_date.isoformat(),
            "table_name": table,
            "transformation_started_at": started_at,
            "transformation_ended_at": ended_at,
            "transformation_status": transformation_status,
            "records_read": records_read,
            "records_output": len(outputs[table]),
            "records_rejected": records_rejected,
            "records_modified": modified,
            "duplicates_discarded": duplicates,
            "rules_applied": dict(rules),
            "exclusions_applied": dict(exclusions),
            "validation_results": detail,
            "output_file": (
                _relative(output_paths[table]) if not settings.dry_run else None
            ),
            "output_checksum": output_checksums.get(table),
            "content_checksum": content_checksums[table],
            "quarantine_file": (
                _relative(quarantine_paths[table]) if table in quarantine_paths else None
            ),
            "error_message": None if transformation_status != STATUS_FAILED else detail,
            "error_code": None if transformation_status != STATUS_FAILED else "VALIDATION_FAIL",
            "transformation_duration_ms": duration_ms,
            "rules_version": RULES_VERSION,
            "null_rates": _null_rates(table, outputs[table]),
            "profile_counts": {
                key: value
                for key, value in ctx.profile_counts.items()
                if any(key.startswith(f"profile_{source}_") for source in sources)
            },
            "input_column_drift": {
                source: ctx.column_drift[source]
                for source in sources
                if source in ctx.column_drift
            },
        }
        table_logs.append(log)
        if not settings.dry_run:
            write_json(settings.log_path(run_date, run_id, table), log)

    ended = datetime.now(PHT)
    summary = {
        "run_id": run_id,
        "parent_run_id": batch.parent_run_id,
        "batch_id": f"{run_id}-b1",
        "batch_window_start": batch.batch_window_start,
        "batch_window_end": batch.batch_window_end,
        "reference_date": settings.reference_date.isoformat(),
        "transformation_started_at": started.isoformat(timespec="seconds"),
        "transformation_ended_at": ended.isoformat(timespec="seconds"),
        "transformation_status": decision.decision,
        "decision": decision.to_dict(),
        "validation_result": report.validation_result,
        "records_read": sum(batch.records_read.values()),
        "records_output": sum(len(rows) for rows in outputs.values()),
        "records_rejected": report.records_rejected,
        "rules_version": RULES_VERSION,
        "tables": table_logs,
        "checks": [check.to_dict() for check in report.checks],
        "profile_counts": ctx.profile_counts,
        "rules_applied": dict(ctx.rules),
        "exclusions_applied": dict(ctx.exclusions),
        "input_column_drift": ctx.column_drift,
    }

    handover_path = None
    validation_path = None
    summary_path = None
    if not settings.dry_run:
        validation_path = settings.transformed_validation_path(run_id)
        write_json(
            validation_path,
            {
                "pipeline_run_id": run_id,
                "parent_run_id": batch.parent_run_id,
                "validation": report.to_dict(),
                "decision": decision.to_dict(),
                "rules_version": RULES_VERSION,
            },
        )
        handover_path = settings.transformed_handover_path(run_id)
        write_json(
            handover_path,
            handover_payload(
                batch,
                ctx,
                report,
                decision,
                {table: _relative(path) for table, path in output_paths.items()},
                output_checksums,
                content_checksums,
                {table: _relative(path) for table, path in quarantine_paths.items()},
                released,
            ),
        )
        summary_path = settings.run_summary_path(run_date, run_id)
        write_json(summary_path, summary)

    return TransformResult(
        run_id=run_id,
        parent_run_id=batch.parent_run_id,
        run_date=run_date,
        decision=decision.decision,
        released=released,
        checks=report.checks,
        outputs=outputs,
        output_paths=output_paths,
        handover_path=handover_path,
        validation_path=validation_path,
        summary_path=summary_path,
        quarantine_paths=quarantine_paths,
    )

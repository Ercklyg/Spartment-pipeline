"""Acceptance decision and handover payloads for Stage 2."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from spartment.common.models import (
    DECISION_APPROVED,
    DECISION_MANUAL_REVIEW,
    DECISION_REJECTED,
    RESULT_FAIL,
    RESULT_PASS,
    RESULT_WARNING,
    AcceptanceDecision,
    ValidationReport,
)
from spartment.stage2_transform.contract import OUTPUT_TABLES, PHT, SOURCE_PRIMARY_OUTPUT
from spartment.stage2_transform.models import Batch, TransformContext

RESPONSIBLE_PARTY = {
    DECISION_APPROVED: "Automated. No human sign-off required.",
    DECISION_MANUAL_REVIEW: (
        "Data Pipeline Maintainer and Super Admin. Review and record a decision "
        "within one business day of the run."
    ),
    DECISION_REJECTED: (
        "Data Pipeline Maintainer and Database Owner. Source-side defects are "
        "resolved by the Database Owner."
    ),
}

PIPELINE_ACTION = {
    DECISION_APPROVED: "Promote transformed datasets, metadata and validation results to the Load Stage.",
    DECISION_MANUAL_REVIEW: (
        "Hold promotion until the warning is reviewed and cleared. Release requires "
        "--allow-manual-review."
    ),
    DECISION_REJECTED: (
        "Stop promotion. Retry transient failures or quarantine the affected transformation batch."
    ),
}


def decide(report: ValidationReport) -> AcceptanceDecision:
    now = datetime.now(PHT).isoformat(timespec="seconds")
    critical = report.critical_failures
    warnings = [
        check
        for check in report.checks
        if check.result == RESULT_WARNING
        or (not check.critical and check.result == RESULT_FAIL)
    ]
    if critical:
        names = ", ".join(check.check_name for check in critical)
        return AcceptanceDecision(
            decision=DECISION_REJECTED,
            decided_at=now,
            rationale=f"Critical check failure: {names}",
            responsible_party=RESPONSIBLE_PARTY[DECISION_REJECTED],
            pipeline_action=PIPELINE_ACTION[DECISION_REJECTED],
            promote=False,
        )
    if warnings:
        names = ", ".join(sorted({check.check_name for check in warnings}))
        return AcceptanceDecision(
            decision=DECISION_MANUAL_REVIEW,
            decided_at=now,
            rationale=f"Non-critical warning requiring review: {names}",
            responsible_party=RESPONSIBLE_PARTY[DECISION_MANUAL_REVIEW],
            pipeline_action=PIPELINE_ACTION[DECISION_MANUAL_REVIEW],
            promote=False,
        )
    return AcceptanceDecision(
        decision=DECISION_APPROVED,
        decided_at=now,
        rationale="All critical checks passed and no warnings were raised.",
        responsible_party=RESPONSIBLE_PARTY[DECISION_APPROVED],
        pipeline_action=PIPELINE_ACTION[DECISION_APPROVED],
        promote=True,
    )


def table_sources(table: str) -> list[str]:
    if table == "dim_tenant":
        return ["profiles", "tenant_assignments"]
    return [
        source for source, target in SOURCE_PRIMARY_OUTPUT.items() if target == table
    ]


def table_status(report: ValidationReport) -> dict[str, str]:
    result: dict[str, str] = {}
    for table in OUTPUT_TABLES:
        sources = table_sources(table)
        relevant = [
            check
            for check in report.checks
            if table in check.target
            or any(source in check.target for source in sources)
            or any(item.get("table") == table for item in check.evidence)
        ]
        if any(check.critical and check.result == RESULT_FAIL for check in relevant):
            result[table] = RESULT_FAIL
        elif any(
            check.result == RESULT_WARNING
            or (not check.critical and check.result == RESULT_FAIL)
            for check in relevant
        ):
            result[table] = RESULT_WARNING
        else:
            result[table] = RESULT_PASS
    return result


def handover_payload(
    batch: Batch,
    ctx: TransformContext,
    report: ValidationReport,
    decision: AcceptanceDecision,
    output_paths: dict[str, str],
    output_checksums: dict[str, str],
    content_checksums: dict[str, str],
    quarantine_paths: dict[str, str],
    released: bool,
) -> dict[str, Any]:
    statuses = table_status(report)
    return {
        "run_id": ctx.run_id,
        "parent_run_id": ctx.parent_run_id,
        "batch_id": f"{ctx.run_id}-b1",
        "batch_window_start": batch.batch_window_start,
        "batch_window_end": batch.batch_window_end,
        "reference_date": ctx.reference_date.isoformat(),
        "rules_version": ctx.rules_version,
        "acceptance_decision": decision.to_dict(),
        "released_for_load": released,
        "validation_result": report.validation_result,
        "transformation_summary": {
            f"{table} transformation": statuses[table] for table in OUTPUT_TABLES
        },
        "extraction_validation_summary": batch.parent_decision,
        "rejection_rate": {
            "rejected": report.records_rejected,
            "duplicates_discarded": sum(ctx.duplicates.values()),
            "records_read": sum(batch.records_read.values()),
            "rate": round(
                report.records_rejected / sum(batch.records_read.values()), 6
            )
            if sum(batch.records_read.values())
            else 0.0,
        },
        "transformed_datasets": output_paths,
        "output_checksums": output_checksums,
        "content_checksums": content_checksums,
        "quarantine_files": quarantine_paths,
        "profile_counts": ctx.profile_counts,
        "rules_applied": dict(ctx.rules),
        "exclusions_applied": dict(ctx.exclusions),
        "input_column_drift": ctx.column_drift,
        "processing_period": batch.processing_period,
    }

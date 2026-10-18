"""Acceptance decision and handover package, extraction.md section 7."""

from __future__ import annotations

from datetime import datetime

from spartment.common.config import Settings
from spartment.common.io import write_json
from spartment.common.models import (
    DECISION_APPROVED,
    DECISION_MANUAL_REVIEW,
    DECISION_REJECTED,
    STATUS_SUCCESS,
    AcceptanceDecision,
    TableRun,
    ValidationReport,
)
from spartment.stage1_extract.contract import standardized_mapping
from spartment.stage1_extract.guard import TablePlan

RESPONSIBLE_PARTY: dict[str, str] = {
    DECISION_APPROVED: "Automated. No human sign-off required.",
    DECISION_MANUAL_REVIEW: (
        "Data Pipeline Maintainer. Review and record a decision within one "
        "business day of the run."
    ),
    DECISION_REJECTED: (
        "Data Pipeline Maintainer and Database Owner. Source side schema or "
        "data defects are resolved by the Database Owner."
    ),
}

PIPELINE_ACTION: dict[str, str] = {
    DECISION_APPROVED: "Promote raw datasets, metadata and validation results to the transformation stage.",
    DECISION_MANUAL_REVIEW: "Hold promotion until the warning is reviewed and cleared.",
    DECISION_REJECTED: "Stop promotion. Retry transient failures or quarantine the affected extract.",
}


def decide(report: ValidationReport) -> AcceptanceDecision:
    now = datetime.now().astimezone().isoformat()

    if report.critical_failures:
        failed = ", ".join(check.check_name for check in report.critical_failures)
        return AcceptanceDecision(
            decision=DECISION_REJECTED,
            decided_at=now,
            rationale=f"Critical check failure: {failed}",
            responsible_party=RESPONSIBLE_PARTY[DECISION_REJECTED],
            pipeline_action=PIPELINE_ACTION[DECISION_REJECTED],
            promote=False,
        )

    if report.warnings:
        warned = ", ".join(check.check_name for check in report.warnings)
        return AcceptanceDecision(
            decision=DECISION_MANUAL_REVIEW,
            decided_at=now,
            rationale=f"Non critical warning requiring review: {warned}",
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


def write_handover(
    settings: Settings,
    runs: list[TableRun],
    plans: list[TablePlan],
    report: ValidationReport,
    decision: AcceptanceDecision,
) -> tuple[object, object]:
    validation_path = settings.validation_path()
    handover_path = settings.handover_path()
    write_json(validation_path, report.to_dict())

    successful = [run for run in runs if run.extraction_status == STATUS_SUCCESS]
    quarantined = [run.table_name for run in runs if run.extraction_status != STATUS_SUCCESS]

    write_json(
        handover_path,
        {
            "pipeline_run_id": settings.run_id,
            "acceptance_decision": decision.to_dict(),
            "validation_result": report.validation_result,
            "validation_summary": {
                f"{run.table_name} table validation": (
                    "PASS" if run.extraction_status == STATUS_SUCCESS else "FAIL"
                )
                for run in runs
            },
            "raw_datasets": {
                run.table_name: run.output_path for run in successful if run.output_path
            },
            "extraction_metadata": [run.to_dict() for run in runs],
            "validation_checks": [check.to_dict() for check in report.checks],
            "rejected_or_quarantined": {
                "tables": quarantined,
                "rows": report.rejected_references,
            },
            "data_contract": {
                "standardized_column_mapping": standardized_mapping(),
                "column_manifest": {plan.spec.name: plan.manifest() for plan in plans if plan.ok},
            },
            "processing_period": settings.run_date.isoformat(),
        },
    )
    return validation_path, handover_path
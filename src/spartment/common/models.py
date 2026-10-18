"""Records produced by every stage of the pipeline.

These are plain dataclasses with no I/O so that later stages can depend on the
shape of an extraction run without depending on the code that produced it.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

STATUS_SUCCESS = "SUCCESS"
STATUS_WARNING = "WARNING"
STATUS_FAILED = "FAILED"

RESULT_PASS = "PASS"
RESULT_WARNING = "WARNING"
RESULT_FAIL = "FAIL"
RESULT_NOT_APPLICABLE = "NOT_APPLICABLE"

DECISION_APPROVED = "APPROVED"
DECISION_MANUAL_REVIEW = "MANUAL REVIEW"
DECISION_REJECTED = "REJECTED"


@dataclass
class TableRun:
    """Extraction result for a single table, section 4.1 of the specification."""

    table_name: str
    extraction_started_at: str
    extraction_ended_at: str
    extraction_status: str
    records_read: int
    records_extracted: int
    records_read_total: int | None
    records_rejected: int
    extraction_window_start: str | None
    extraction_window_end: str
    validation_result: str
    error_message: str | None
    error_code: str | None
    extraction_duration_ms: int
    output_path: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CheckResult:
    """One extraction validation check, section 3 of the specification."""

    check_name: str
    target: str
    purpose: str
    result: str
    detail: str
    critical: bool
    evidence: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ValidationReport:
    """Outcome of every check for one run."""

    pipeline_run_id: str
    validation_started_at: str
    validation_ended_at: str
    validation_duration_ms: int
    validation_result: str
    records_rejected: int
    checks: list[CheckResult]
    rejected_references: list[dict[str, Any]]

    @property
    def critical_failures(self) -> list[CheckResult]:
        return [c for c in self.checks if c.critical and c.result == RESULT_FAIL]

    @property
    def warnings(self) -> list[CheckResult]:
        return [c for c in self.checks if c.result == RESULT_WARNING]

    def to_dict(self) -> dict[str, Any]:
        return {
            "pipeline_run_id": self.pipeline_run_id,
            "validation_started_at": self.validation_started_at,
            "validation_ended_at": self.validation_ended_at,
            "validation_duration_ms": self.validation_duration_ms,
            "validation_result": self.validation_result,
            "records_rejected": self.records_rejected,
            "checks": [check.to_dict() for check in self.checks],
            "rejected_references": self.rejected_references,
        }


@dataclass
class AcceptanceDecision:
    """Section 7 acceptance decision for one run."""

    decision: str
    decided_at: str
    rationale: str
    responsible_party: str
    pipeline_action: str
    promote: bool
    quarantined_tables: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
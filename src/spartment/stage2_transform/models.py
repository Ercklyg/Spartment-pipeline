"""Runtime records and exceptions for Stage 2."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

from spartment.stage2_transform.contract import SOURCE_OUTPUT_TABLES


class TransformError(RuntimeError):
    """Base class for transformation failures."""


class TransformConfigError(TransformError):
    """Configuration is missing or malformed."""


class TransformInputError(TransformError):
    """The parent handover or staged input cannot be used."""


class TransformRetryableError(TransformInputError):
    """A file-read or parse failure that may succeed on retry."""


class TransformSchemaError(TransformInputError):
    """The staged input does not match the Stage 1 contract."""


@dataclass
class QuarantineRecord:
    source_table: str
    output_table: str
    rule_id: str
    rejection_reason: str
    source_values: dict[str, Any]
    mask_fields: tuple[str, ...] = ()


@dataclass
class Batch:
    parent_run_id: str
    processing_period: str
    handover: dict[str, Any]
    metadata: dict[str, Any]
    validation: dict[str, Any]
    rows: dict[str, list[dict[str, str]]]
    headers: dict[str, tuple[str, ...]]
    manifest: dict[str, Any]
    records_read: dict[str, int]
    parent_decision: str
    batch_window_start: str
    batch_window_end: str
    reference_date: date
    column_drift: dict[str, dict[str, list[str]]] = field(default_factory=dict)


@dataclass
class TransformContext:
    run_id: str
    parent_run_id: str
    run_date: str
    reference_date: date
    rules_version: str
    rules: Counter[str] = field(default_factory=Counter)
    rules_by_table: dict[str, Counter[str]] = field(default_factory=dict)
    rules_by_output: dict[str, Counter[str]] = field(default_factory=dict)
    exclusions: Counter[str] = field(default_factory=Counter)
    exclusions_by_table: dict[str, Counter[str]] = field(default_factory=dict)
    exclusions_by_output: dict[str, Counter[str]] = field(default_factory=dict)
    quarantine: dict[str, list[QuarantineRecord]] = field(default_factory=dict)
    duplicates: Counter[str] = field(default_factory=Counter)
    profile_counts: dict[str, int] = field(default_factory=dict)
    column_drift: dict[str, dict[str, list[str]]] = field(default_factory=dict)
    modified: Counter[str] = field(default_factory=Counter)
    dangling_references: list[dict[str, Any]] = field(default_factory=list)
    source_dates: set[date] = field(default_factory=set)

    def record_rule(
        self,
        rule_id: str,
        source_table: str,
        amount: int = 1,
        output_table: str | None = None,
    ) -> None:
        self.rules[rule_id] += amount
        self.rules_by_table.setdefault(source_table, Counter())[rule_id] += amount
        outputs = (
            (output_table,)
            if output_table is not None
            else SOURCE_OUTPUT_TABLES.get(source_table, (source_table,))
        )
        for table in outputs:
            self.rules_by_output.setdefault(table, Counter())[rule_id] += amount

    def record_exclusion(
        self,
        exclusion_id: str,
        source_table: str,
        amount: int = 1,
        output_table: str | None = None,
    ) -> None:
        self.exclusions[exclusion_id] += amount
        self.exclusions_by_table.setdefault(source_table, Counter())[exclusion_id] += amount
        outputs = (
            (output_table,)
            if output_table is not None
            else SOURCE_OUTPUT_TABLES.get(source_table, (source_table,))
        )
        for table in outputs:
            self.exclusions_by_output.setdefault(table, Counter())[exclusion_id] += amount

    def record_modified(self, source_table: str, amount: int = 1) -> None:
        self.modified[source_table] += amount

    def record_dangling_reference(
        self,
        source_table: str,
        output_table: str,
        row: dict[str, Any],
        parent_name: str,
    ) -> None:
        self.dangling_references.append(
            {
                "table": output_table,
                "column": parent_name,
                "value": None,
                "parent": parent_name,
                "source_table": source_table,
                "source_id": row.get("id"),
                "reason": "referenced parent member was rejected or is unavailable",
            }
        )

    def reject(
        self,
        source_table: str,
        output_table: str,
        row: dict[str, Any],
        rule_id: str,
        reason: str,
        mask_fields: tuple[str, ...] = (),
    ) -> None:
        self.record_rule(rule_id, source_table, output_table=output_table)
        self.quarantine.setdefault(source_table, []).append(
            QuarantineRecord(
                source_table=source_table,
                output_table=output_table,
                rule_id=rule_id,
                rejection_reason=reason,
                source_values=dict(row),
                mask_fields=mask_fields,
            )
        )

    def record_duplicate(
        self,
        source_table: str,
        output_table: str,
        row: dict[str, Any],
        reason: str,
        mask_fields: tuple[str, ...] = (),
    ) -> None:
        self.record_rule("C-06", source_table, output_table=output_table)
        self.duplicates[source_table] += 1
        self.quarantine.setdefault(source_table, []).append(
            QuarantineRecord(
                source_table=source_table,
                output_table=output_table,
                rule_id="C-06",
                rejection_reason=reason,
                source_values=dict(row),
                mask_fields=mask_fields,
            )
        )

    def rejections(self, source_table: str, include_duplicates: bool = True) -> list[QuarantineRecord]:
        records = self.quarantine.get(source_table, [])
        if include_duplicates:
            return list(records)
        return [record for record in records if record.rule_id != "C-06"]


@dataclass(frozen=True)
class TransformSettings:
    raw_root: Path
    output_root: Path
    log_root: Path
    quarantine_root: Path
    reference_date: date
    allow_manual_review: bool = False
    dry_run: bool = False

    @property
    def handover_dir(self) -> Path:
        return self.raw_root / "_handover"

    @property
    def metadata_dir(self) -> Path:
        return self.raw_root / "_metadata"

    @property
    def validation_dir(self) -> Path:
        return self.raw_root / "_validation"

    def output_dir(self, run_date: str) -> Path:
        return self.output_root

    def output_table_dir(self, table: str, run_date: str) -> Path:
        return self.output_root / table / run_date

    def output_path(self, table: str, run_date: str) -> Path:
        return self.output_table_dir(table, run_date) / f"{table}_{run_date}.csv"

    def log_dir(self, run_date: str) -> Path:
        return self.log_root / run_date

    def log_path(self, run_date: str, run_id: str, table: str) -> Path:
        return self.log_dir(run_date) / f"{run_id}_{table}.json"

    def run_summary_path(self, run_date: str, run_id: str) -> Path:
        return self.log_dir(run_date) / f"{run_id}.json"

    def quarantine_path(self, run_date: str, table: str) -> Path:
        return self.quarantine_root / run_date / f"{table}_rejected.csv"

    def transformed_handover_path(self, run_id: str) -> Path:
        return self.output_root / "_handover" / f"{run_id}.json"

    def transformed_validation_path(self, run_id: str) -> Path:
        return self.output_root / "_validation" / f"{run_id}.json"


@dataclass
class TransformResult:
    run_id: str
    parent_run_id: str
    run_date: str
    decision: str
    released: bool
    checks: list[Any]
    outputs: dict[str, list[dict[str, Any]]]
    output_paths: dict[str, Path]
    handover_path: Path | None
    validation_path: Path | None
    summary_path: Path | None
    quarantine_paths: dict[str, Path]

    @property
    def exit_code(self) -> int:
        from spartment.common.models import DECISION_APPROVED, DECISION_MANUAL_REVIEW

        if self.decision == DECISION_APPROVED:
            return 0
        if self.decision == DECISION_MANUAL_REVIEW:
            return 0 if self.released else 5
        return 1

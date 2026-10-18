"""Extraction level validation, extraction.md section 3.

Validation runs against the staged CSV datasets rather than a second live
read, so it judges exactly the artefact handed to the transformation stage.
Checks are deliberately scoped to retrieval integrity. Cleaning,
standardisation and business calculations are not performed here.

Criticality is taken from the section 7.1 enumeration of critical failures:
a missing required table or column, duplicate primary keys, incomplete
extraction, and connectivity loss.
"""

from __future__ import annotations

import csv
import time
from collections.abc import Iterator, Sequence
from datetime import date as date_cls
from typing import Any

from spartment.common.config import Settings
from spartment.common.models import (
    RESULT_FAIL as FAIL,
    RESULT_NOT_APPLICABLE as NOT_APPLICABLE,
    RESULT_PASS as PASS,
    RESULT_WARNING as WARNING,
    STATUS_SUCCESS,
    CheckResult,
    TableRun,
    ValidationReport,
)
from spartment.stage1_extract.contract import (
    ASSIGNMENT_ROOM_INVARIANT,
    DATE_COLUMNS,
    DATE_ORDERING_RULES,
    ENUM_COLUMNS,
    MONETARY_COLUMNS,
    NUMERIC_BOUNDS,
    RELATIONSHIPS,
    REQUIRED_IDENTIFIERS,
)
from spartment.stage1_extract.guard import TablePlan

EVIDENCE_LIMIT = 50


def _rows(path: Any) -> Iterator[dict[str, str]]:
    with path.open("r", encoding="utf-8", newline="") as handle:
        yield from csv.DictReader(handle)


def _cap(evidence: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return evidence[:EVIDENCE_LIMIT]


def _blank(value: str | None) -> bool:
    return value is None or value.strip() == ""


def _as_float(value: str) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_date(value: str) -> date_cls | None:
    try:
        return date_cls.fromisoformat(value.strip()[:10])
    except (TypeError, ValueError):
        return None


def validate(
    settings: Settings,
    runs: Sequence[TableRun],
    plans: Sequence[TablePlan],
) -> ValidationReport:
    started = settings.now()
    started_perf = time.perf_counter()

    by_table = {run.table_name: run for run in runs}
    quarantined = [plan for plan in plans if not plan.ok]
    extractable = [
        plan
        for plan in plans
        if plan.ok and by_table.get(plan.spec.name, None) is not None
        and by_table[plan.spec.name].extraction_status == STATUS_SUCCESS
    ]

    checks: list[CheckResult] = []
    rejected_references: list[dict[str, Any]] = []
    rejected_total = 0

    checks.append(
        CheckResult(
            check_name="source_connection_accessibility",
            target="Supabase PostgREST endpoint",
            purpose="Confirm the source is reachable and serving schema and rows",
            critical=True,
            result=PASS if extractable else FAIL,
            detail=(
                f"{len(extractable)} of {len(plans)} tables were extracted"
                if extractable
                else "no table could be read from the source"
            ),
        )
    )

    checks.append(
        CheckResult(
            check_name="required_tables_and_columns_exist",
            target="Extraction contract versus live schema",
            purpose="Detect schema drift before promotion",
            critical=True,
            result=PASS if not quarantined else FAIL,
            detail=(
                "all required tables and columns are present"
                if not quarantined
                else f"{len(quarantined)} table(s) do not satisfy the contract"
            ),
            evidence=[
                {"table_name": plan.spec.name, "detail": plan.reason}
                for plan in quarantined
            ],
        )
    )

    failed_runs = [run for run in runs if run.extraction_status != STATUS_SUCCESS]
    checks.append(
        CheckResult(
            check_name="extraction_errors_or_interruptions",
            target="Extraction run metadata",
            purpose="Identify partial or failed extractions",
            critical=True,
            result=PASS if not failed_runs else FAIL,
            detail=(
                "no table reported an extraction failure"
                if not failed_runs
                else f"{len(failed_runs)} table(s) did not extract cleanly"
            ),
            evidence=[
                {
                    "table_name": run.table_name,
                    "error_code": run.error_code,
                    "error_message": run.error_message,
                }
                for run in failed_runs
            ],
        )
    )

    identifiers_evidence: list[dict[str, Any]] = []
    identifiers_count = 0
    duplicates_evidence: list[dict[str, Any]] = []
    duplicates_count = 0
    count_evidence: list[dict[str, Any]] = []
    count_mismatch = 0
    numeric_evidence: list[dict[str, Any]] = []
    numeric_count = 0
    date_evidence: list[dict[str, Any]] = []
    date_count = 0
    invariant_evidence: list[dict[str, Any]] = []
    invariant_count = 0
    bounds_evidence: list[dict[str, Any]] = []
    bounds_count = 0
    domains: dict[str, set[str]] = {f"{t}.{c}": set() for t, c in ENUM_COLUMNS}
    identifiers_seen: dict[str, set[str]] = {}

    invariant_table, invariant_column = ASSIGNMENT_ROOM_INVARIANT

    for plan in extractable:
        table = plan.spec.name
        run = by_table[table]
        path = settings.table_directory(table) / f"{table}.csv"
        selected = set(plan.selected)
        required = [c for c in REQUIRED_IDENTIFIERS.get(table, ()) if c in selected]
        monetary = [c for c in MONETARY_COLUMNS.get(table, ()) if c in selected]
        bounds = {c: b for c, b in NUMERIC_BOUNDS.get(table, {}).items() if c in selected}
        table_domains = {
            column: domains[f"{table}.{column}"]
            for enum_table, column in ENUM_COLUMNS
            if enum_table == table and column in selected
        }

        seen: set[str] = set()
        row_count = 0

        for row_number, row in enumerate(_rows(path), start=2):
            row_count += 1
            identifier = row.get("id", "")
            if not _blank(identifier):
                if identifier in seen:
                    duplicates_count += 1
                    if len(duplicates_evidence) < EVIDENCE_LIMIT:
                        duplicates_evidence.append(
                            {"table_name": table, "id": identifier, "csv_line": row_number}
                        )
                else:
                    seen.add(identifier)

            for column in required:
                if _blank(row.get(column)):
                    rejected_total += 1
                    identifiers_count += 1
                    entry = {
                        "table_name": table,
                        "csv_line": row_number,
                        "id": identifier or None,
                        "column": column,
                        "reason": "required_identifier_null",
                    }
                    rejected_references.append(entry)
                    if len(identifiers_evidence) < EVIDENCE_LIMIT:
                        identifiers_evidence.append(entry)

            for column in monetary:
                value = row.get(column, "")
                if _blank(value):
                    continue
                amount = _as_float(value)
                if amount is None:
                    numeric_count += 1
                    if len(numeric_evidence) < EVIDENCE_LIMIT:
                        numeric_evidence.append(
                            {"table_name": table, "column": column, "value": value, "reason": "not_numeric"}
                        )
                elif amount < 0:
                    numeric_count += 1
                    if len(numeric_evidence) < EVIDENCE_LIMIT:
                        numeric_evidence.append(
                            {"table_name": table, "column": column, "value": value, "reason": "negative_amount"}
                        )

            for column, (low, high) in bounds.items():
                value = row.get(column, "")
                if _blank(value):
                    continue
                size = _as_float(value)
                if size is None or not (low <= size <= high):
                    bounds_count += 1
                    if len(bounds_evidence) < EVIDENCE_LIMIT:
                        bounds_evidence.append(
                            {
                                "table_name": table,
                                "column": column,
                                "value": value,
                                "expected_range": f"{low}..{high}",
                            }
                        )

            for column, bucket in table_domains.items():
                value = row.get(column)
                if not _blank(value):
                    bucket.add(value.strip())

            if table == invariant_table:
                status = (row.get("status") or "").strip()
                room = row.get(invariant_column, "")
                violation = (
                    (status == "active" and _blank(room))
                    or (status == "ended" and not _blank(room))
                )
                if violation:
                    invariant_count += 1
                    if len(invariant_evidence) < EVIDENCE_LIMIT:
                        invariant_evidence.append(
                            {
                                "table_name": table,
                                "id": identifier or None,
                                "status": status,
                                "room_id": room or None,
                            }
                        )

        identifiers_seen[table] = seen

        total = run.records_read_total
        if total is not None and total != row_count:
            count_mismatch += 1
            if len(count_evidence) < EVIDENCE_LIMIT:
                count_evidence.append(
                    {
                        "table_name": table,
                        "source_total": total,
                        "staged_rows": row_count,
                        "difference": row_count - total,
                    }
                )

    for plan in extractable:
        table = plan.spec.name
        date_columns = [c for c in DATE_COLUMNS.get(table, ()) if c in plan.selected]
        if not date_columns:
            continue
        path = settings.table_directory(table) / f"{table}.csv"
        for row_number, row in enumerate(_rows(path), start=2):
            for column in date_columns:
                value = row.get(column, "")
                if _blank(value):
                    continue
                if _as_date(value) is None:
                    date_count += 1
                    if len(date_evidence) < EVIDENCE_LIMIT:
                        date_evidence.append(
                            {
                                "table_name": table,
                                "csv_line": row_number,
                                "column": column,
                                "value": value,
                                "reason": "unparseable_date",
                            }
                        )

    for table, later_column, earlier_column, rule in DATE_ORDERING_RULES:
        spec_plan = next((p for p in extractable if p.spec.name == table), None)
        if spec_plan is None or later_column not in spec_plan.selected or earlier_column not in spec_plan.selected:
            continue
        path = settings.table_directory(table) / f"{table}.csv"
        for row in _rows(path):
            later, earlier = row.get(later_column, ""), row.get(earlier_column, "")
            if _blank(later) or _blank(earlier):
                continue
            parsed_later, parsed_earlier = _as_date(later), _as_date(earlier)
            if parsed_later is None or parsed_earlier is None:
                continue
            if parsed_later < parsed_earlier:
                date_count += 1
                if len(date_evidence) < EVIDENCE_LIMIT:
                    date_evidence.append(
                        {
                            "table_name": table,
                            "column": later_column,
                            "value": later,
                            "compared_to": earlier_column,
                            "compared_value": earlier,
                            "reason": f"violates {rule}",
                        }
                    )

    unresolved_evidence: list[dict[str, Any]] = []
    unresolved_count = 0
    unavailable = [rel for rel in RELATIONSHIPS if not rel.available]
    for rel in RELATIONSHIPS:
        if not rel.available:
            continue
        child_plan = next((p for p in extractable if p.spec.name == rel.child_table), None)
        parent_ids = identifiers_seen.get(rel.parent_table)
        if child_plan is None or parent_ids is None or rel.child_column not in child_plan.selected:
            continue
        path = settings.table_directory(rel.child_table) / f"{rel.child_table}.csv"
        for row in _rows(path):
            value = row.get(rel.child_column, "")
            if _blank(value):
                continue
            if value not in parent_ids:
                unresolved_count += 1
                if len(unresolved_evidence) < EVIDENCE_LIMIT:
                    unresolved_evidence.append(
                        {
                            "child_table": rel.child_table,
                            "child_column": rel.child_column,
                            "child_id": row.get("id") or None,
                            "value": value,
                            "missing_parent_table": rel.parent_table,
                        }
                    )

    def outcome(failures: int, warnings: int) -> str:
        if failures:
            return FAIL
        if warnings:
            return WARNING
        return PASS

    checks.append(
        CheckResult(
            check_name="required_identifiers_present",
            target="Primary keys and required relationship fields",
            purpose="Ensure records can be traced and related downstream",
            critical=False,
            result=outcome(0, identifiers_count),
            detail=(
                "no required identifier is null"
                if not identifiers_count
                else f"{identifiers_count} null required identifier(s) across {len(extractable)} table(s)"
            ),
            evidence=_cap(identifiers_evidence),
        )
    )

    checks.append(
        CheckResult(
            check_name="primary_key_uniqueness",
            target="id column of each extracted table",
            purpose="Detect duplicate identifiers introduced by extraction",
            critical=True,
            result=outcome(duplicates_count, 0),
            detail="no duplicate primary keys" if not duplicates_count else f"{duplicates_count} duplicate primary key(s)",
            evidence=_cap(duplicates_evidence),
        )
    )

    checks.append(
        CheckResult(
            check_name="record_count_completeness",
            target="Staged row count versus source row count",
            purpose="Detect incomplete extraction or unexpected emptiness",
            critical=True,
            result=outcome(0, count_mismatch),
            detail=(
                "staged row counts match the source for every table"
                if not count_mismatch
                else f"{count_mismatch} table(s) show a count difference"
            ),
            evidence=_cap(count_evidence),
        )
    )

    checks.append(
        CheckResult(
            check_name="referential_completeness",
            target="Relationship columns across the extracted tables",
            purpose="Detect orphaned references before downstream joins",
            critical=False,
            result=outcome(0, unresolved_count),
            detail=(
                f"{unresolved_count} unresolved reference(s); "
                f"{len(unavailable)} relationship target(s) excluded from the decision "
                "because auth.users is out of extraction scope"
            ),
            evidence=_cap(
                unresolved_evidence
                + [
                    {
                        "child_table": rel.child_table,
                        "child_column": rel.child_column,
                        "missing_parent_table": rel.parent_table,
                        "reason": "target_table_unavailable",
                    }
                    for rel in unavailable
                ]
            ),
        )
    )

    checks.append(
        CheckResult(
            check_name="numeric_range_sanity",
            target="Monetary and size columns",
            purpose="Flag obviously invalid negative or unparseable amounts",
            critical=False,
            result=outcome(0, numeric_count),
            detail="no negative or unparseable monetary values" if not numeric_count else f"{numeric_count} suspicious value(s)",
            evidence=_cap(numeric_evidence),
        )
    )

    checks.append(
        CheckResult(
            check_name="date_sanity",
            target="Date columns and their ordering relationships",
            purpose="Flag invalid or inconsistent dates",
            critical=False,
            result=outcome(0, date_count),
            detail="all dates are parseable and consistently ordered" if not date_count else f"{date_count} suspicious date(s)",
            evidence=_cap(date_evidence),
        )
    )

    checks.append(
        CheckResult(
            check_name="assignment_room_invariant",
            target=f"{invariant_table}.status and {invariant_table}.{invariant_column}",
            purpose="Confirm active tenancies hold a room and ended ones do not",
            critical=False,
            result=outcome(0, invariant_count),
            detail=(
                "assignment status and room reference are consistent"
                if not invariant_count
                else f"{invariant_count} assignment(s) violate the invariant"
            ),
            evidence=_cap(invariant_evidence),
        )
    )

    checks.append(
        CheckResult(
            check_name="room_size_bounds",
            target="rooms.size_sqm",
            purpose="Confirm room sizes remain inside the database constraint",
            critical=False,
            result=outcome(0, bounds_count),
            detail="all room sizes are within the permitted range" if not bounds_count else f"{bounds_count} room(s) outside the permitted range",
            evidence=_cap(bounds_evidence),
        )
    )

    observed = {key: sorted(values) for key, values in domains.items() if values}
    checks.append(
        CheckResult(
            check_name="observed_enum_domains",
            target="Enum typed status and role columns",
            purpose=(
                "Report the value domains actually observed. PostgREST exposes enum type "
                "names but not their members, so domains are reported rather than asserted."
            ),
            critical=False,
            result=NOT_APPLICABLE if observed else WARNING,
            detail=f"{len(observed)} enum column(s) observed" if observed else "no enum values were observed",
            evidence=[{"column": key, "values": value} for key, value in observed.items()],
        )
    )

    if any(check.critical and check.result == FAIL for check in checks):
        overall = FAIL
    elif any(check.result == WARNING for check in checks):
        overall = WARNING
    else:
        overall = PASS

    ended = settings.now()
    return ValidationReport(
        pipeline_run_id=settings.run_id,
        validation_started_at=started.isoformat(),
        validation_ended_at=ended.isoformat(),
        validation_duration_ms=int((time.perf_counter() - started_perf) * 1000),
        validation_result=overall,
        records_rejected=rejected_total,
        checks=checks,
        rejected_references=rejected_references[:EVIDENCE_LIMIT],
    )

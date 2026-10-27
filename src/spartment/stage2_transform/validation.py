"""Transformation validation checks V-01 through V-25."""

from __future__ import annotations

import json
import re
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any

from spartment.common.models import (
    RESULT_FAIL as FAIL,
    RESULT_PASS as PASS,
    RESULT_WARNING as WARNING,
    CheckResult,
    ValidationReport,
)
from spartment.stage2_transform.contract import (
    CODE_DOMAINS,
    DATE_PATTERN,
    EXCLUSION_IDS,
    FK_COLUMNS,
    NATURAL_KEY,
    NON_NEGATIVE,
    OUTPUT_COLUMNS,
    OUTPUT_TABLES,
    OUTPUT_TYPES,
    PHT,
    REQUIRED_NOT_NULL,
    SOURCE_PRIMARY_OUTPUT,
    SOURCE_TABLES,
    SURROGATE_KEY,
    TIMESTAMP_PATTERN,
)
from spartment.stage2_transform.models import Batch, TransformContext, TransformSettings
from spartment.stage2_transform.standardize import normalize_uuid, parse_decimal
from spartment.stage2_transform.writer import content_checksum

EVIDENCE_LIMIT = 50
MONEY_PATTERN = re.compile(r"^-?\d+\.\d{2}$")
RATE_PATTERN = re.compile(r"^-?\d+\.\d{4}$")
INT_PATTERN = re.compile(r"^-?\d+$")
SURROGATE_PATTERN = re.compile(r"^[0-9a-f]{16}$")
BILLING_PERIOD_RE = re.compile(r"^\d{4}-\d{2}$")

JOIN_CARDINALITY: tuple[tuple[str, str, str], ...] = (
    ("J-01", "rooms", "dim_room"),
    ("J-02", "leases", "dim_lease"),
    ("J-03", "leases", "dim_lease"),
    ("J-04", "tenant_assignments", "fact_assignment"),
    ("J-05", "tenant_assignments", "fact_assignment"),
    ("J-06", "bills", "fact_billing"),
    ("J-07", "payments", "fact_payment"),
    ("J-08", "payment_corrections", "fact_payment_correction"),
    ("J-09", "inquiries", "fact_inquiry"),
    ("J-10", "tenant_requests", "fact_tenant_request"),
    ("J-11", "tenant_requests", "fact_tenant_request"),
    ("J-12", "tenant_occupancy_events", "fact_occupancy_event"),
    ("J-13", "tenant_occupancy_events", "fact_occupancy_event"),
)

OPTIONAL_COLUMNS: dict[str, tuple[str, ...]] = {
    "dim_tenant": ("phone", "email", "lease_end"),
    "dim_lease": ("lease_end_date",),
    "fact_assignment": ("room_key", "lease_end"),
    "fact_payment": ("reference_note",),
    "fact_inquiry": ("room_key", "phone"),
    "fact_tenant_request": ("room_key",),
}


def _add(
    checks: list[CheckResult],
    check_name: str,
    target: str,
    purpose: str,
    result: str,
    detail: str,
    critical: bool,
    evidence: list[dict[str, Any]] | None = None,
) -> None:
    checks.append(
        CheckResult(
            check_name=check_name,
            target=target,
            purpose=purpose,
            result=result,
            detail=detail,
            critical=critical,
            evidence=(evidence or [])[:EVIDENCE_LIMIT],
        )
    )


def _dec(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _as_date(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value))
    except (TypeError, ValueError):
        return None


def _as_datetime(value: Any) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value))
        return parsed if parsed.tzinfo is not None else None
    except (TypeError, ValueError):
        return None


def _duplicates(values: list[Any]) -> Counter[Any]:
    counter = Counter(values)
    return Counter({key: count for key, count in counter.items() if count > 1})


def _evidence_duplicates(table: str, column: str, duplicates: Counter[Any]) -> list[dict[str, Any]]:
    return [
        {"table": table, "column": column, "value": key, "count": count}
        for key, count in duplicates.most_common(EVIDENCE_LIMIT)
    ]


def _rejected_sum(batch: Batch, ctx: TransformContext, table: str, column: str) -> Decimal:
    total = Decimal("0")
    for record in ctx.rejections(table):
        value = parse_decimal(record.source_values.get(column))
        if value is not None:
            total += value
    return total


def _raw_sum(batch: Batch, table: str, column: str) -> Decimal:
    total = Decimal("0")
    for row in batch.rows[table]:
        value = parse_decimal(row.get(column))
        if value is not None:
            total += value
    return total


def _output_sum(rows: list[dict[str, Any]], column: str) -> Decimal:
    total = Decimal("0")
    for row in rows:
        value = _dec(row.get(column))
        if value is not None:
            total += value
    return total


def _validate_type(value: Any, kind: str, table: str, column: str) -> bool:
    if value is None or value == "":
        return True
    text = str(value)
    if kind == "surrogate":
        return bool(SURROGATE_PATTERN.fullmatch(text))
    if kind == "uuid":
        return normalize_uuid(text) == text
    if kind == "date":
        return bool(re.fullmatch(DATE_PATTERN, text)) and _as_date(text) is not None
    if kind == "timestamp":
        parsed = _as_datetime(text)
        return (
            parsed is not None
            and parsed.utcoffset() == timedelta(hours=8)
            and bool(re.fullmatch(TIMESTAMP_PATTERN, text))
        )
    if kind in {"money", "area"}:
        return bool(MONEY_PATTERN.fullmatch(text)) and _dec(text) is not None
    if kind == "rate":
        return bool(RATE_PATTERN.fullmatch(text)) and _dec(text) is not None
    if kind == "int":
        return bool(INT_PATTERN.fullmatch(text))
    if kind == "bool":
        return text in {"TRUE", "FALSE"}
    if kind == "code":
        if table == "dim_date" and column == "date_key":
            return bool(re.fullmatch(r"\d{8}", text))
        return bool(text)
    return True


def _previous_log(settings: TransformSettings, table: str, run_id: str) -> dict[str, Any] | None:
    candidates = [
        path
        for path in settings.log_root.rglob(f"*_{table}.json")
        if not path.name.startswith(f"{run_id}_")
    ]
    if not candidates:
        return None
    path = max(candidates, key=lambda item: item.stat().st_mtime)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    return payload if isinstance(payload, dict) else None


def validate(
    outputs: dict[str, list[dict[str, Any]]],
    batch: Batch,
    ctx: TransformContext,
    settings: TransformSettings,
) -> ValidationReport:
    started = datetime.now(PHT)
    started_perf = time.perf_counter()
    checks: list[CheckResult] = []

    source_to_output = SOURCE_PRIMARY_OUTPUT
    for table in SOURCE_TABLES:
        output_table = source_to_output[table]
        if table == "profiles":
            output_count = sum(
                1
                for row in outputs["dim_tenant"]
                if row.get("identity_source") in {"PROFILE", "BOTH"}
            )
        else:
            output_count = len(outputs[output_table])
        rejected = len(ctx.rejections(table, include_duplicates=False))
        duplicates = ctx.duplicates[table]
        expected = output_count + rejected + duplicates
        records_read = batch.records_read.get(table, 0)
        passed = expected == records_read
        _add(
            checks,
            "V-01 Record reconciliation",
            table,
            "Detect lost records; prove nothing vanished silently",
            PASS if passed else FAIL,
            (
                f"{table}: {records_read} read = {output_count} output + "
                f"{rejected} rejected + {duplicates} duplicates"
                if passed
                else (
                    f"{table}: {records_read} read != {output_count} output + "
                    f"{rejected} rejected + {duplicates} duplicates"
                )
            ),
            True,
            [] if passed else [{"table": table, "records_read": records_read, "expected": expected}],
        )

    duplicate_total = sum(ctx.duplicates.values())
    unexpected = [
        {"table": table, "count": count}
        for table, count in ctx.duplicates.items()
        if count and table not in {"payments", "tenant_assignments"}
    ]
    duplicate_ok = not unexpected and duplicate_total == ctx.rules.get("C-06", 0)
    _add(
        checks,
        "V-02 Duplicate accounting",
        "payments, tenant_assignments",
        "Prove removed duplicates are accounted for",
        PASS if duplicate_ok else FAIL,
        (
            f"{duplicate_total} duplicate(s) discarded, all from permitted tables"
            if duplicate_ok
            else f"unexpected duplicate accounting: {unexpected or ctx.rules.get('C-06', 0)}"
        ),
        True,
        unexpected,
    )

    key_duplicates: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        key_column = "date_key" if table == "dim_date" else SURROGATE_KEY.get(table)
        if not key_column:
            continue
        duplicates = _duplicates([row.get(key_column) for row in outputs[table]])
        if duplicates:
            key_duplicates.extend(_evidence_duplicates(table, key_column, duplicates))
    _add(
        checks,
        "V-03 Surrogate key uniqueness",
        "Every *_key",
        "Prevent row multiplication",
        PASS if not key_duplicates else FAIL,
        "0 duplicate surrogate keys" if not key_duplicates else f"{len(key_duplicates)} duplicate key value(s)",
        True,
        key_duplicates,
    )

    natural_duplicates: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        key_column = NATURAL_KEY[table]
        duplicates = _duplicates([row.get(key_column) for row in outputs[table]])
        if duplicates:
            natural_duplicates.extend(_evidence_duplicates(table, key_column, duplicates))
    _add(
        checks,
        "V-04 Natural key uniqueness",
        "Every *_id",
        "Preserve source grain",
        PASS if not natural_duplicates else FAIL,
        "0 duplicate natural keys" if not natural_duplicates else f"{len(natural_duplicates)} duplicate natural key value(s)",
        True,
        natural_duplicates,
    )

    null_failures: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        for column in REQUIRED_NOT_NULL[table]:
            count = sum(1 for row in outputs[table] if row.get(column) is None or row.get(column) == "")
            if count:
                null_failures.append({"table": table, "column": column, "count": count})
    _add(
        checks,
        "V-05 Required columns NOT NULL",
        "Every NOT NULL column in Section 6.1",
        "Catch rows that escaped C-13 with a missing key or measure",
        PASS if not null_failures else FAIL,
        "0 NULLs in required columns" if not null_failures else f"{len(null_failures)} column(s) contain NULLs",
        True,
        null_failures,
    )

    parent_keys: dict[str, set[Any]] = {}
    for table in OUTPUT_TABLES:
        key_column = "date_key" if table == "dim_date" else SURROGATE_KEY.get(table)
        if key_column:
            parent_keys[table] = {row.get(key_column) for row in outputs[table] if row.get(key_column)}
    fk_failures: list[dict[str, Any]] = []
    for table, relationships in FK_COLUMNS.items():
        for column, parent, nullable in relationships:
            allowed = parent_keys.get(parent, set())
            for row in outputs[table]:
                value = row.get(column)
                if value is None:
                    if not nullable:
                        fk_failures.append(
                            {"table": table, "column": column, "value": None, "parent": parent}
                        )
                    continue
                if value not in allowed:
                    fk_failures.append(
                        {"table": table, "column": column, "value": value, "parent": parent}
                    )
    fk_failures.extend(dict(item) for item in ctx.dangling_references)
    _add(
        checks,
        "V-06 Referential integrity",
        "Every non-null FK in Section 6.1",
        "Prevent orphans in the transformed model",
        PASS if not fk_failures else FAIL,
        "100% of non-null FKs resolve" if not fk_failures else f"{len(fk_failures)} unresolved FK value(s)",
        True,
        fk_failures,
    )

    parent_duplicates: list[dict[str, Any]] = []
    for parent in {parent for relationships in FK_COLUMNS.values() for _, parent, _ in relationships}:
        key_column = SURROGATE_KEY[parent]
        duplicates = _duplicates([row.get(key_column) for row in outputs[parent]])
        if duplicates:
            parent_duplicates.extend(_evidence_duplicates(parent, key_column, duplicates))

    def accepted_count(source: str) -> int:
        read = batch.records_read.get(source, 0)
        rejected = len(ctx.rejections(source, include_duplicates=False))
        return max(read - rejected - ctx.duplicates[source], 0)

    join_failures: list[dict[str, Any]] = []
    for join_id, source, output_table in JOIN_CARDINALITY:
        expected = accepted_count(source)
        actual = len(outputs[output_table])
        if actual != expected:
            join_failures.append(
                {
                    "join": join_id,
                    "left": source,
                    "output": output_table,
                    "accepted_left_rows": expected,
                    "post_join_rows": actual,
                }
            )
    join_ok = not parent_duplicates and not join_failures
    _add(
        checks,
        "V-07 Join cardinality",
        "Joins J-01...J-13",
        "Catch non-unique lookup keys or row loss during joins",
        PASS if join_ok else FAIL,
        (
            f"{len(JOIN_CARDINALITY)} joins preserve accepted left-row counts; "
            "all right-hand keys are unique"
            if join_ok
            else (
                f"{len(parent_duplicates)} duplicate parent key value(s); "
                f"{len(join_failures)} join row-count mismatch(es)"
            )
        ),
        True,
        parent_duplicates + join_failures,
    )

    negative: list[dict[str, Any]] = []
    for table, columns in NON_NEGATIVE.items():
        for column in columns:
            for row in outputs[table]:
                value = _dec(row.get(column))
                if value is not None and value < 0:
                    negative.append({"table": table, "column": column, "value": str(value)})
    _add(
        checks,
        "V-08 Non-negative measures",
        "money measures",
        "Enforce commercial invariants",
        PASS if not negative else FAIL,
        "0 negative measures" if not negative else f"{len(negative)} negative measure(s)",
        True,
        negative,
    )

    total_failures: list[dict[str, Any]] = []
    balance_failures: list[dict[str, Any]] = []
    for row in outputs["fact_billing"]:
        rent = _dec(row.get("rent_amount"))
        electricity = _dec(row.get("electricity_amount"))
        water = _dec(row.get("water_amount"))
        total = _dec(row.get("total_amount"))
        paid = _dec(row.get("amount_paid"))
        balance = _dec(row.get("balance"))
        if None not in (rent, electricity, water, total):
            if abs(total - (rent + electricity + water)) > Decimal("0.01"):
                total_failures.append({"bill_id": row.get("bill_id"), "total": str(total)})
        if None not in (total, paid, balance):
            if abs(balance - max(total - paid, Decimal("0"))) > Decimal("0.01"):
                balance_failures.append({"bill_id": row.get("bill_id"), "balance": str(balance)})
    _add(
        checks,
        "V-09 Bill total identity",
        "fact_billing.total_amount",
        "Prove the total is the sum of its parts",
        PASS if not total_failures else FAIL,
        "bill totals reconcile" if not total_failures else f"{len(total_failures)} bill total mismatch(es)",
        True,
        total_failures,
    )
    _add(
        checks,
        "V-10 Bill balance identity",
        "fact_billing.balance",
        "Prove the payment ledger reconciles",
        PASS if not balance_failures else FAIL,
        "bill balances reconcile" if not balance_failures else f"{len(balance_failures)} balance mismatch(es)",
        True,
        balance_failures,
    )

    date_failures: list[dict[str, Any]] = []
    for row in outputs["fact_billing"]:
        due = _as_date(row.get("due_date"))
        period = _as_date(row.get("billing_period_date"))
        if due and period and due < period:
            date_failures.append({"table": "fact_billing", "bill_id": row.get("bill_id")})
    for table, start_column, end_column in (
        ("dim_lease", "move_in_date", "lease_end_date"),
        ("fact_assignment", "move_in", "lease_end"),
    ):
        for row in outputs[table]:
            start = _as_date(row.get(start_column))
            end = _as_date(row.get(end_column))
            if start and end and end < start:
                date_failures.append(
                    {"table": table, "id": row.get(NATURAL_KEY[table]), "start": str(start), "end": str(end)}
                )
    _add(
        checks,
        "V-11 Date ordering",
        "billing period, due date, lease and assignment dates",
        "Catch impossible date states after C-07/C-08",
        PASS if not date_failures else FAIL,
        "all date orderings are valid" if not date_failures else f"{len(date_failures)} date ordering violation(s)",
        True,
        date_failures,
    )

    invariant_failures: list[dict[str, Any]] = []
    for row in outputs["fact_assignment"]:
        status = row.get("status")
        room_key = row.get("room_key")
        if status == "ACTIVE" and room_key is None:
            invariant_failures.append({"assignment_id": row.get("assignment_id"), "status": status})
        if status == "ENDED" and room_key is not None:
            invariant_failures.append({"assignment_id": row.get("assignment_id"), "status": status})
    _add(
        checks,
        "V-12 Assignment-room invariant",
        "fact_assignment.status, room_key",
        "Enforce the source invariant",
        PASS if not invariant_failures else FAIL,
        "assignment room invariant holds" if not invariant_failures else f"{len(invariant_failures)} violation(s)",
        True,
        invariant_failures,
    )

    raw_payments = _raw_sum(batch, "payments", "amount")
    expected_payments = raw_payments - _rejected_sum(batch, ctx, "payments", "amount")
    output_payments = _output_sum(outputs["fact_payment"], "amount")
    payment_ok = abs(expected_payments - output_payments) <= Decimal("0.01")
    _add(
        checks,
        "V-13 Payment reconciliation",
        "SUM(fact_payment.amount) vs staged payments.amount",
        "Prove collections were not silently altered",
        PASS if payment_ok else FAIL,
        (
            f"{output_payments} reconciles to {expected_payments} net of rejections"
            if payment_ok
            else f"{output_payments} != {expected_payments} net of rejections"
        ),
        True,
        [] if payment_ok else [{"expected": str(expected_payments), "actual": str(output_payments)}],
    )

    component_failures: list[dict[str, Any]] = []
    for column in ("rent_amount", "electricity_amount", "water_amount"):
        expected = _raw_sum(batch, "bills", column) - _rejected_sum(batch, ctx, "bills", column)
        actual = _output_sum(outputs["fact_billing"], column)
        if abs(expected - actual) > Decimal("0.01"):
            component_failures.append(
                {
                    "table": "fact_billing",
                    "component": column,
                    "expected": str(expected),
                    "actual": str(actual),
                }
            )
    _add(
        checks,
        "V-14 Bill component reconciliation",
        "SUM of each bill component",
        "Prove cleaning did not alter billed amounts",
        PASS if not component_failures else FAIL,
        "bill components reconcile" if not component_failures else f"{len(component_failures)} component mismatch(es)",
        True,
        component_failures,
    )

    missing_exclusions = [exclusion for exclusion in EXCLUSION_IDS if exclusion not in ctx.exclusions]
    expected_exclusions = {
        "X-01": (
            sum(
                1
                for row in outputs["dim_tenant"]
                if row.get("is_tenant") == "TRUE" and row.get("active_flag") == "FALSE"
            )
            + sum(1 for row in outputs["dim_lease"] if row.get("is_active") == "FALSE")
            + sum(1 for row in outputs["fact_assignment"] if row.get("is_active") == "FALSE")
        ),
        "X-02": sum(
            1
            for row in outputs["fact_billing"]
            if row.get("status") != "PAID" or _dec(row.get("balance")) not in (None, Decimal("0"))
        ),
        "X-03": len(outputs["fact_payment_correction"]),
        "X-04": sum(
            1
            for table in SOURCE_TABLES
            for record in ctx.rejections(table)
            if record.rule_id == "X-04"
        ),
        "X-05": sum(
            1
            for row in outputs["fact_inquiry"] + outputs["fact_tenant_request"]
            if row.get("status") == "RESOLVED"
        ),
        "X-06": sum(1 for row in outputs["dim_tenant"] if row.get("role") == "ADMIN"),
    }
    exclusion_mismatches = [
        {
            "exclusion": exclusion,
            "recorded": ctx.exclusions.get(exclusion, 0),
            "expected": expected,
        }
        for exclusion, expected in expected_exclusions.items()
        if ctx.exclusions.get(exclusion, 0) != expected
    ]
    exclusion_ok = not missing_exclusions and not exclusion_mismatches
    _add(
        checks,
        "V-15 Exclusion accounting",
        "X-01...X-06 counters",
        "Prove business exclusions are counted and reported",
        PASS if exclusion_ok else FAIL,
        (
            "every exclusion has a counter and the counts match affected rows"
            if exclusion_ok
            else (
                f"missing exclusion counter(s): {', '.join(missing_exclusions)}"
                if missing_exclusions
                else f"{len(exclusion_mismatches)} exclusion count mismatch(es)"
            )
        ),
        False,
        exclusion_mismatches
        if exclusion_mismatches
        else [] if not missing_exclusions else [{"missing": missing_exclusions}],
    )

    type_failures: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        for column in OUTPUT_COLUMNS[table]:
            kind = OUTPUT_TYPES[table].get(column, "text")
            for row in outputs[table]:
                value = row.get(column)
                if not _validate_type(value, kind, table, column):
                    type_failures.append(
                        {"table": table, "column": column, "value": value, "expected": kind}
                    )
                    break
    _add(
        checks,
        "V-16 Type / format conformance",
        "Every output column vs Section 6.1",
        "Confirm CSV values parse as the contracted type",
        PASS if not type_failures else FAIL,
        "0 values fail parse or format" if not type_failures else f"{len(type_failures)} column(s) have invalid values",
        False,
        type_failures,
    )

    domain_failures: list[dict[str, Any]] = []
    for table, columns in CODE_DOMAINS.items():
        for column, allowed in columns.items():
            for row in outputs[table]:
                value = row.get(column)
                if value not in allowed:
                    domain_failures.append(
                        {"table": table, "column": column, "value": value, "allowed": sorted(allowed)}
                    )
                    break
    for table in ("dim_date", "fact_billing"):
        for row in outputs[table]:
            value = row.get("billing_period")
            if not BILLING_PERIOD_RE.fullmatch(str(value or "")):
                domain_failures.append({"table": table, "column": "billing_period", "value": value})
                break
    _add(
        checks,
        "V-17 Code domain conformance",
        "code sets",
        "Confirm code sets are respected",
        PASS if not domain_failures else FAIL,
        "all code values are in domain" if not domain_failures else f"{len(domain_failures)} out-of-domain value(s)",
        False,
        domain_failures,
    )

    null_baseline_failures: list[dict[str, Any]] = []
    for table, columns in OPTIONAL_COLUMNS.items():
        rows = outputs[table]
        if not rows:
            continue
        previous = _previous_log(settings, table, ctx.run_id)
        previous_rates = previous.get("null_rates", {}) if previous else {}
        for column in columns:
            nulls = sum(1 for row in rows if row.get(column) is None or row.get(column) == "")
            current_rate = nulls / len(rows)
            previous_rate = previous_rates.get(column) if isinstance(previous_rates, dict) else None
            if current_rate > 0.90:
                null_baseline_failures.append(
                    {"table": table, "column": column, "rate": round(current_rate, 4), "reason": ">90%"}
                )
            elif previous_rate is not None and current_rate > float(previous_rate):
                null_baseline_failures.append(
                    {
                        "table": table,
                        "column": column,
                        "rate": round(current_rate, 4),
                        "previous": previous_rate,
                        "reason": "increased",
                    }
                )
    null_failure = any(item.get("reason") == ">90%" for item in null_baseline_failures)
    _add(
        checks,
        "V-18 Null rate on optional columns",
        "optional columns",
        "Track degradation of optional data",
        FAIL if null_failure else WARNING if null_baseline_failures else PASS,
        "optional null rates are stable" if not null_baseline_failures else f"{len(null_baseline_failures)} optional-column regression(s)",
        False,
        null_baseline_failures,
    )

    unmapped_checks = 0
    for table in OUTPUT_TABLES:
        if "code_unmapped" not in OUTPUT_COLUMNS[table]:
            continue
        rows = outputs[table]
        if not rows:
            continue
        count = sum(1 for row in rows if row.get("code_unmapped") == "TRUE")
        rate_value = count / len(rows)
        if rate_value > 0.05:
            result = FAIL
        elif rate_value > 0.01:
            result = WARNING
        else:
            result = PASS
        evidence = {
            "table": table,
            "unmapped": count,
            "rows": len(rows),
            "rate": round(rate_value, 4),
        }
        _add(
            checks,
            "V-19 Unmapped code rate",
            table,
            "Detect new code variants",
            result,
            f"{table}: {count}/{len(rows)} rows unmapped ({rate_value:.2%})",
            False,
            [evidence],
        )
        unmapped_checks += 1
    if not unmapped_checks:
        _add(
            checks,
            "V-19 Unmapped code rate",
            "code_unmapped flags",
            "Detect new code variants",
            PASS,
            "no code-bearing output rows",
            False,
        )

    per_table_rates: list[dict[str, Any]] = []
    total_rejected = 0
    total_read = 0
    for table in SOURCE_TABLES:
        read = batch.records_read.get(table, 0)
        rejected = len(ctx.rejections(table, include_duplicates=False))
        duplicates = ctx.duplicates[table]
        total_rejected += rejected
        total_read += read
        rate_value = rejected / read if read else 0.0
        if rate_value > 0.05:
            result = FAIL
        elif rate_value > 0.01:
            result = WARNING
        else:
            result = PASS
        evidence = {
            "table": table,
            "rejected": rejected,
            "duplicates_discarded": duplicates,
            "read": read,
            "rate": round(rate_value, 4),
        }
        per_table_rates.append(evidence)
        _add(
            checks,
            "V-20 Rejection rate",
            table,
            "Decide acceptance against the pre-declared threshold",
            result,
            f"{table}: {rejected}/{read} rejected ({rate_value:.2%}); "
            f"{duplicates} duplicate(s) discarded separately",
            result == FAIL,
            [evidence],
        )
    overall_rate = total_rejected / total_read if total_read else 0.0
    overall_result = FAIL if overall_rate > 0.05 else WARNING if overall_rate > 0.01 else PASS
    _add(
        checks,
        "V-20 Rejection rate",
        "overall",
        "Decide acceptance against the pre-declared threshold",
        overall_result,
        f"overall: {total_rejected}/{total_read} rejected ({overall_rate:.2%})",
        overall_result == FAIL,
        [{"rejected": total_rejected, "read": total_read, "rate": round(overall_rate, 4)}],
    )

    current_checksums = {table: content_checksum(table, outputs[table]) for table in OUTPUT_TABLES}
    idempotency: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        previous = _previous_log(settings, table, ctx.run_id)
        if not previous or previous.get("parent_run_id") != batch.parent_run_id:
            continue
        previous_checksum = previous.get("content_checksum")
        previous_version = previous.get("rules_version")
        if previous_version != ctx.rules_version:
            continue
        if previous_checksum and previous_checksum != current_checksums[table]:
            idempotency.append(
                {
                    "table": table,
                    "reason": "content checksum changed",
                    "previous": previous_checksum,
                    "current": current_checksums[table],
                }
            )
    _add(
        checks,
        "V-21 Idempotency assertion",
        "repeat run content checksums",
        "Prove Section 1.4's claim",
        WARNING if idempotency else PASS,
        "content checksums are stable" if not idempotency else f"{len(idempotency)} idempotency difference(s)",
        False,
        idempotency,
    )

    lineage_failures: list[dict[str, Any]] = []
    for table in OUTPUT_TABLES:
        if "transformation_run_id" not in OUTPUT_COLUMNS[table]:
            continue
        count = sum(1 for row in outputs[table] if row.get("transformation_run_id") != ctx.run_id)
        if count:
            lineage_failures.append({"table": table, "count": count})
    _add(
        checks,
        "V-22 Lineage completeness",
        "transformation_run_id",
        "Make every output row traceable",
        PASS if not lineage_failures else FAIL,
        "every output row carries this run_id" if not lineage_failures else f"{len(lineage_failures)} table(s) have incomplete lineage",
        True,
        lineage_failures,
    )

    sensitive_findings: list[dict[str, Any]] = []
    header_patterns = (
        "password",
        "passwd",
        "secret",
        "service_role",
        "supabase_service_role_key",
        "api_key",
        "apikey",
        "token",
    )
    value_patterns = (
        "postgres://",
        "postgresql://",
        "service_role",
        "supabase_service_role_key",
        "password=",
        "secret=",
        "api_key=",
    )
    for table in OUTPUT_TABLES:
        for column in OUTPUT_COLUMNS[table]:
            lowered = column.lower()
            if any(pattern in lowered for pattern in header_patterns):
                sensitive_findings.append({"table": table, "column": column, "reason": "sensitive header"})
        for row in outputs[table]:
            for column, value in row.items():
                text = str(value).lower() if value is not None else ""
                if any(pattern in text for pattern in value_patterns):
                    sensitive_findings.append(
                        {"table": table, "column": column, "reason": "sensitive value"}
                    )
                    break
    _add(
        checks,
        "V-23 Sensitive data absence",
        "all output files",
        "Ensure no credential or auth.users payload leaks",
        PASS if not sensitive_findings else FAIL,
        "0 credential fields or DSN fragments" if not sensitive_findings else f"{len(sensitive_findings)} sensitive finding(s)",
        True,
        sensitive_findings,
    )

    size_failures: list[dict[str, Any]] = []
    for row in outputs["dim_room"]:
        size = _dec(row.get("size_sqm"))
        expected = size is not None and (size < Decimal("8") or size > Decimal("14"))
        actual = row.get("size_out_of_bounds") == "TRUE"
        if expected != actual:
            size_failures.append(
                {"room_id": row.get("room_id"), "size_sqm": row.get("size_sqm"), "flag": row.get("size_out_of_bounds")}
            )
    _add(
        checks,
        "V-24 Room size bounds",
        "dim_room.size_out_of_bounds",
        "Confirm the flag matches the source value",
        PASS if not size_failures else FAIL,
        "room size flags match the source" if not size_failures else f"{len(size_failures)} flag disagreement(s)",
        False,
        size_failures,
    )

    active_lease_room_ids = {
        row["room_id"]
        for row in outputs["dim_lease"]
        if row.get("is_active") == "TRUE"
    }
    active_assignment_room_keys = {
        row["room_key"]
        for row in outputs["fact_assignment"]
        if row.get("is_active") == "TRUE" and row.get("room_key")
    }
    active_assignment_room_ids = {
        row["room_id"]
        for row in outputs["dim_room"]
        if row.get("room_key") in active_assignment_room_keys
    }
    active_room_ids = active_lease_room_ids | active_assignment_room_ids
    occupancy_failures: list[dict[str, Any]] = []
    for room in outputs["dim_room"]:
        room_id = room.get("room_id")
        status = room.get("status")
        has_active = room_id in active_room_ids
        if status == "OCCUPIED" and not has_active:
            occupancy_failures.append({"room_id": room_id, "status": status, "issue": "no active lease"})
        if status == "AVAILABLE" and has_active:
            occupancy_failures.append({"room_id": room_id, "status": status, "issue": "active lease exists"})
    _add(
        checks,
        "V-25 Occupancy consistency",
        "dim_room, dim_lease, fact_assignment",
        "Detect rooms whose status contradicts the tenancy records",
        WARNING if occupancy_failures else PASS,
        "room status agrees with tenancy records" if not occupancy_failures else f"{len(occupancy_failures)} contradiction(s)",
        False,
        occupancy_failures,
    )

    critical_failures = [check for check in checks if check.critical and check.result == FAIL]
    warning_failures = [
        check
        for check in checks
        if check.result == WARNING or (not check.critical and check.result == FAIL)
    ]
    if critical_failures:
        overall = FAIL
    elif warning_failures:
        overall = WARNING
    else:
        overall = PASS

    ended = datetime.now(PHT)
    return ValidationReport(
        pipeline_run_id=ctx.run_id,
        validation_started_at=started.isoformat(timespec="seconds"),
        validation_ended_at=ended.isoformat(timespec="seconds"),
        validation_duration_ms=int((time.perf_counter() - started_perf) * 1000),
        validation_result=overall,
        records_rejected=total_rejected,
        checks=checks,
        rejected_references=[
            {
                "table": record.source_table,
                "output_table": record.output_table,
                "rule_id": record.rule_id,
                "reason": record.rejection_reason,
            }
            for table in SOURCE_TABLES
            for record in ctx.rejections(table)
        ][:500],
    )

"""Cleaning rules C-01 through C-13."""

from __future__ import annotations

from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from spartment.stage2_transform.contract import (
    AREA_FIELDS,
    BOOL_FIELDS,
    CODE_FIELDS,
    CODE_MAPS,
    DATE_FIELDS,
    EMAIL_FIELDS,
    INPUT_COLUMNS,
    JSON_FIELDS,
    MONEY_FIELDS,
    OUTPUT_TABLES,
    REQUIRED_NOT_NULL,
    TIMESTAMP_FIELDS,
    UUID_FIELDS,
)
from spartment.stage2_transform.models import TransformContext
from spartment.stage2_transform.standardize import (
    canonical_json,
    clean_text,
    lower_text,
    money,
    normalize_uuid,
    parse_bool,
    parse_date,
    parse_decimal,
    parse_timestamp,
)

MONEY_COLUMNS = {
    (table, column)
    for table, columns in MONEY_FIELDS.items()
    for column in columns
}
AREA_COLUMNS = {
    (table, column)
    for table, columns in AREA_FIELDS.items()
    for column in columns
}
DATE_COLUMNS = {
    (table, column)
    for table, columns in DATE_FIELDS.items()
    for column in columns
}
TIMESTAMP_COLUMNS = {
    (table, column)
    for table, columns in TIMESTAMP_FIELDS.items()
    for column in columns
}
BOOL_COLUMNS = {
    (table, column)
    for table, columns in BOOL_FIELDS.items()
    for column in columns
}
JSON_COLUMNS = {
    (table, column)
    for table, columns in JSON_FIELDS.items()
    for column in columns
}
UUID_COLUMNS = {
    (table, column)
    for table, columns in UUID_FIELDS.items()
    for column in columns
}
EMAIL_COLUMNS = {
    (table, column)
    for table, columns in EMAIL_FIELDS.items()
    for column in columns
}


def _code_value(table: str, column: str, raw: object) -> tuple[str, bool, str | None]:
    key_map = dict(CODE_FIELDS.get(table, ()))
    key = key_map.get(column)
    text = clean_text(raw)
    if key is None:
        return text or "UNKNOWN", False, text
    if text is None:
        return "UNKNOWN", True, None
    mapped = CODE_MAPS[key].get(text.lower())
    if mapped is None:
        return "UNKNOWN", True, text
    return mapped, False, text


def _reject(
    ctx: TransformContext,
    table: str,
    output_table: str,
    raw: dict[str, str],
    rule_id: str,
    reason: str,
) -> None:
    mask: tuple[str, ...] = ()
    if table in {"profiles", "tenant_assignments", "inquiries"}:
        mask = ("phone",)
    ctx.reject(table, output_table, raw, rule_id, reason, mask)


def _primary_output(table: str) -> str:
    return {
        "profiles": "dim_tenant",
        "buildings": "dim_building",
        "rooms": "dim_room",
        "leases": "dim_lease",
        "tenant_assignments": "fact_assignment",
        "bills": "fact_billing",
        "payments": "fact_payment",
        "payment_corrections": "fact_payment_correction",
        "inquiries": "fact_inquiry",
        "tenant_requests": "fact_tenant_request",
        "tenant_occupancy_events": "fact_occupancy_event",
        "notifications": "fact_notification",
    }[table]


def clean_table(table: str, rows: list[dict[str, str]], ctx: TransformContext) -> list[dict[str, Any]]:
    cleaned: list[dict[str, Any]] = []
    output_table = _primary_output(table)
    code_map = dict(CODE_FIELDS.get(table, ()))

    for raw in rows:
        row: dict[str, Any] = {"_raw": raw, "_unmapped": False}
        rejected = False
        rule_hits: set[str] = set()
        row_modified = False

        for column in INPUT_COLUMNS[table]:
            source = raw.get(column)
            cleaned_text = source if (table, column) in JSON_COLUMNS else clean_text(source)

            if (table, column) in JSON_COLUMNS:
                if source is None or str(source).strip() == "":
                    row[column] = None
                    row[f"_{column}_valid"] = True
                else:
                    value, valid = canonical_json(source)
                    row[column] = value
                    row[f"_{column}_valid"] = valid
                    original = str(source)
                    if not valid or value != original:
                        rule_hits.add("C-12")
                    if value != original:
                        row_modified = True
            elif (table, column) in DATE_COLUMNS:
                if cleaned_text is not None:
                    parsed = parse_date(cleaned_text)
                    if parsed is None:
                        _reject(
                            ctx,
                            table,
                            output_table,
                            raw,
                            "S-01",
                            f"unparseable date in {column}: {source!r}",
                        )
                        rejected = True
                        break
                else:
                    parsed = None
                if parsed is not None:
                    ctx.source_dates.add(parsed)
                row[column] = parsed
            elif (table, column) in TIMESTAMP_COLUMNS:
                if cleaned_text is not None:
                    parsed_dt = parse_timestamp(cleaned_text)
                    if parsed_dt is None:
                        _reject(
                            ctx,
                            table,
                            output_table,
                            raw,
                            "S-01",
                            f"unparseable timestamp in {column}: {source!r}",
                        )
                        rejected = True
                        break
                else:
                    parsed_dt = None
                if parsed_dt is not None:
                    ctx.source_dates.add(parsed_dt.date())
                row[column] = parsed_dt
            elif (table, column) in MONEY_COLUMNS:
                if cleaned_text is not None:
                    parsed_money = parse_decimal(cleaned_text)
                    if parsed_money is None or parsed_money < 0:
                        _reject(
                            ctx,
                            table,
                            output_table,
                            raw,
                            "C-05",
                            f"invalid or negative money in {column}: {source!r}",
                        )
                        rejected = True
                        break
                else:
                    parsed_money = None
                row[column] = parsed_money
            elif (table, column) in AREA_COLUMNS:
                if cleaned_text is not None:
                    parsed_area = parse_decimal(cleaned_text)
                    if parsed_area is None:
                        _reject(
                            ctx,
                            table,
                            output_table,
                            raw,
                            "S-02",
                            f"unparseable area in {column}: {source!r}",
                        )
                        rejected = True
                        break
                else:
                    parsed_area = None
                row[column] = parsed_area
            elif (table, column) in BOOL_COLUMNS:
                if cleaned_text is not None:
                    parsed_bool = parse_bool(cleaned_text)
                    if parsed_bool is None:
                        _reject(
                            ctx,
                            table,
                            output_table,
                            raw,
                            "S-06",
                            f"unparseable boolean in {column}: {source!r}",
                        )
                        rejected = True
                        break
                else:
                    parsed_bool = None
                row[column] = parsed_bool
            elif (table, column) in UUID_COLUMNS:
                if cleaned_text is not None:
                    parsed_uuid = normalize_uuid(cleaned_text)
                    if parsed_uuid is None:
                        _reject(
                            ctx,
                            table,
                            output_table,
                            raw,
                            "S-01",
                            f"invalid UUID in {column}: {source!r}",
                        )
                        rejected = True
                        break
                else:
                    parsed_uuid = None
                row[column] = parsed_uuid
            elif column in code_map:
                value, unmapped, raw_value = _code_value(table, column, source)
                row[column] = value
                row[f"_{column}_raw"] = raw_value
                row[f"_{column}_unmapped"] = unmapped
                if unmapped:
                    row["_unmapped"] = True
                    rule_hits.add("C-11")
                    row_modified = True
                elif raw_value is not None and raw_value != value:
                    rule_hits.add("C-11")
                    row_modified = True
            else:
                row[column] = cleaned_text
                if (
                    source is not None
                    and str(source) != ""
                    and cleaned_text != str(source)
                ):
                    rule_hits.add("C-01")
                    row_modified = True

        if rejected:
            continue

        if (table, "email") in EMAIL_COLUMNS:
            email = lower_text(raw.get("email"))
            original = clean_text(raw.get("email"))
            row["email"] = email
            if original is not None and email != original:
                rule_hits.add("C-02")
                row_modified = True

        if table == "bills":
            period = row.get("billing_period")
            due = row.get("due_date")
            if isinstance(period, date) and isinstance(due, date) and due < period:
                row["due_date"] = period + timedelta(days=15)
                row["_due_date_corrected"] = True
                rule_hits.add("C-07")
                row_modified = True
            else:
                row["_due_date_corrected"] = False

        if table in {"leases", "tenant_assignments"}:
            start_column = "move_in_date" if table == "leases" else "move_in"
            end_column = "lease_end_date" if table == "leases" else "lease_end"
            start = row.get(start_column)
            end = row.get(end_column)
            if isinstance(start, date) and isinstance(end, date) and end < start:
                row[end_column] = None
                row["_date_corrected"] = True
                rule_hits.add("C-08")
                row_modified = True
            else:
                row["_date_corrected"] = False

        if table == "tenant_assignments":
            if row.get("status") == "ACTIVE" and row.get("room_id") is None:
                _reject(ctx, table, output_table, raw, "C-09", "ACTIVE assignment has no room")
                continue
            if row.get("status") == "ENDED" and row.get("room_id") is not None:
                _reject(ctx, table, output_table, raw, "C-09", "ENDED assignment still has a room")
                continue

        for rule_id in rule_hits:
            ctx.record_rule(rule_id, table)
        row["_modified"] = row_modified
        cleaned.append(row)

    return cleaned


def _created_at_key(row: dict[str, Any]) -> float:
    value = row.get("created_at")
    if isinstance(value, datetime):
        return value.timestamp()
    return 0.0


def deduplicate_assignments(
    rows: list[dict[str, Any]], ctx: TransformContext
) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (_created_at_key(row), str(row.get("id", ""))),
        reverse=True,
    )
    survivors: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in ordered:
        source_id = str(row.get("id", ""))
        if source_id in seen:
            ctx.record_duplicate(
                "tenant_assignments",
                "fact_assignment",
                row["_raw"],
                f"duplicate assignment id {source_id}; kept latest created_at",
                mask_fields=("phone",),
            )
            continue
        seen.add(source_id)
        survivors.append(row)
    return survivors


def deduplicate_payments(
    rows: list[dict[str, Any]], ctx: TransformContext
) -> list[dict[str, Any]]:
    ordered = sorted(
        rows,
        key=lambda row: (
            _created_at_key(row),
            str(row.get("id", "")),
        ),
    )
    survivors: list[dict[str, Any]] = []
    seen: dict[tuple[str, str, str], str] = {}
    for row in ordered:
        key = (
            str(row.get("bill_id", "")),
            str(row.get("payment_date", "")),
            str(money(row.get("amount"))),
        )
        survivor_id = seen.get(key)
        if survivor_id is not None:
            ctx.record_duplicate(
                "payments",
                "fact_payment",
                row["_raw"],
                "duplicate payment (bill_id, payment_date, amount); "
                f"kept earliest created_at id={survivor_id}",
            )
            continue
        seen[key] = str(row.get("id", ""))
        survivors.append(row)
    return survivors


def reject_orphans(
    cleaned: dict[str, list[dict[str, Any]]], ctx: TransformContext
) -> dict[str, list[dict[str, Any]]]:
    accepted: dict[str, list[dict[str, Any]]] = {}

    def reject_child(
        table: str,
        output_table: str,
        row: dict[str, Any],
        column: str,
        parent: str,
    ) -> None:
        _reject(
            ctx,
            table,
            output_table,
            row["_raw"],
            "C-10",
            f"{column} {row.get(column)!r} has no parent in {parent}",
        )

    def reject_required(
        table: str,
        output_table: str,
        row: dict[str, Any],
        column: str,
    ) -> None:
        _reject(
            ctx,
            table,
            output_table,
            row["_raw"],
            "C-13",
            f"required reference {column} is NULL",
        )

    buildings = cleaned["buildings"]
    accepted["buildings"] = buildings
    building_ids = {row["id"] for row in buildings if row.get("id")}

    rooms: list[dict[str, Any]] = []
    for row in cleaned["rooms"]:
        if row.get("building_id") is None:
            reject_required("rooms", "dim_room", row, "building_id")
            continue
        if row["building_id"] not in building_ids:
            reject_child("rooms", "dim_room", row, "building_id", "buildings")
            continue
        rooms.append(row)
    accepted["rooms"] = rooms
    room_ids = {row["id"] for row in rooms if row.get("id")}

    profiles = cleaned["profiles"]
    accepted["profiles"] = profiles
    profile_ids = {row["id"] for row in profiles if row.get("id")}

    assignments: list[dict[str, Any]] = []
    for row in cleaned["tenant_assignments"]:
        if row.get("room_id") is not None and row["room_id"] not in room_ids:
            reject_child("tenant_assignments", "fact_assignment", row, "room_id", "rooms")
            continue
        assignments.append(row)
    accepted["tenant_assignments"] = assignments

    leases: list[dict[str, Any]] = []
    for row in cleaned["leases"]:
        if row.get("tenant_id") is None:
            reject_required("leases", "dim_lease", row, "tenant_id")
            continue
        if row["tenant_id"] not in profile_ids:
            reject_child("leases", "dim_lease", row, "tenant_id", "profiles")
            continue
        if row.get("room_id") is None:
            reject_required("leases", "dim_lease", row, "room_id")
            continue
        if row["room_id"] not in room_ids:
            reject_child("leases", "dim_lease", row, "room_id", "rooms")
            continue
        leases.append(row)
    accepted["leases"] = leases
    lease_ids = {row["id"] for row in leases if row.get("id")}

    bills: list[dict[str, Any]] = []
    for row in cleaned["bills"]:
        if row.get("lease_id") is None:
            reject_required("bills", "fact_billing", row, "lease_id")
            continue
        if row["lease_id"] not in lease_ids:
            reject_child("bills", "fact_billing", row, "lease_id", "leases")
            continue
        bills.append(row)
    accepted["bills"] = bills
    bill_ids = {row["id"] for row in bills if row.get("id")}

    payments: list[dict[str, Any]] = []
    for row in cleaned["payments"]:
        if row.get("bill_id") is None:
            reject_required("payments", "fact_payment", row, "bill_id")
            continue
        if row["bill_id"] not in bill_ids:
            reject_child("payments", "fact_payment", row, "bill_id", "bills")
            continue
        payments.append(row)
    accepted["payments"] = payments
    payment_ids = {row["id"] for row in payments if row.get("id")}

    corrections: list[dict[str, Any]] = []
    for row in cleaned["payment_corrections"]:
        if row.get("payment_id") is None:
            reject_required(
                "payment_corrections",
                "fact_payment_correction",
                row,
                "payment_id",
            )
            continue
        if row["payment_id"] not in payment_ids:
            reject_child(
                "payment_corrections",
                "fact_payment_correction",
                row,
                "payment_id",
                "payments",
            )
            continue
        corrections.append(row)
    accepted["payment_corrections"] = corrections

    inquiries: list[dict[str, Any]] = []
    for row in cleaned["inquiries"]:
        if row.get("room_id") is not None and row["room_id"] not in room_ids:
            reject_child("inquiries", "fact_inquiry", row, "room_id", "rooms")
            continue
        inquiries.append(row)
    accepted["inquiries"] = inquiries

    requests: list[dict[str, Any]] = []
    for row in cleaned["tenant_requests"]:
        if row.get("tenant_id") is None:
            reject_required("tenant_requests", "fact_tenant_request", row, "tenant_id")
            continue
        if row["tenant_id"] not in profile_ids:
            reject_child("tenant_requests", "fact_tenant_request", row, "tenant_id", "profiles")
            continue
        if row.get("room_id") is not None and row["room_id"] not in room_ids:
            reject_child("tenant_requests", "fact_tenant_request", row, "room_id", "rooms")
            continue
        requests.append(row)
    accepted["tenant_requests"] = requests

    events: list[dict[str, Any]] = []
    for row in cleaned["tenant_occupancy_events"]:
        if row.get("tenant_id") is None:
            reject_required(
                "tenant_occupancy_events", "fact_occupancy_event", row, "tenant_id"
            )
            continue
        if row["tenant_id"] not in profile_ids:
            reject_child(
                "tenant_occupancy_events", "fact_occupancy_event", row, "tenant_id", "profiles"
            )
            continue
        if row.get("previous_lease_id") is None:
            reject_required(
                "tenant_occupancy_events",
                "fact_occupancy_event",
                row,
                "previous_lease_id",
            )
            continue
        if row["previous_lease_id"] not in lease_ids:
            reject_child(
                "tenant_occupancy_events",
                "fact_occupancy_event",
                row,
                "previous_lease_id",
                "leases",
            )
            continue
        if row.get("new_lease_id") is not None and row["new_lease_id"] not in lease_ids:
            reject_child(
                "tenant_occupancy_events",
                "fact_occupancy_event",
                row,
                "new_lease_id",
                "leases",
            )
            continue
        if row.get("performed_by") is None:
            reject_required(
                "tenant_occupancy_events",
                "fact_occupancy_event",
                row,
                "performed_by",
            )
            continue
        if row["performed_by"] not in profile_ids:
            reject_child(
                "tenant_occupancy_events",
                "fact_occupancy_event",
                row,
                "performed_by",
                "profiles",
            )
            continue
        events.append(row)
    accepted["tenant_occupancy_events"] = events

    notifications: list[dict[str, Any]] = []
    for row in cleaned["notifications"]:
        if row.get("tenant_id") is None:
            reject_required("notifications", "fact_notification", row, "tenant_id")
            continue
        if row["tenant_id"] not in profile_ids:
            reject_child("notifications", "fact_notification", row, "tenant_id", "profiles")
            continue
        notifications.append(row)
    accepted["notifications"] = notifications

    return accepted


def finalize(
    ctx: TransformContext,
    source_table: str,
    output_table: str,
    source_row: dict[str, Any],
    output_row: dict[str, Any],
) -> dict[str, Any] | None:
    missing = [
        column
        for column in REQUIRED_NOT_NULL[output_table]
        if output_row.get(column) is None
    ]
    if missing:
        _reject(
            ctx,
            source_table,
            output_table,
            source_row["_raw"],
            "C-13",
            f"required output column(s) are NULL: {', '.join(missing)}",
        )
        return None
    if source_row.get("_modified") and not source_row.get("_modified_counted"):
        ctx.record_modified(source_table)
        source_row["_modified_counted"] = True
    return output_row

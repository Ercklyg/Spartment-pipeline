"""Fact builders and post-fact enrichment."""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from spartment.stage2_transform.cleaning import finalize
from spartment.stage2_transform.contract import ENTITY
from spartment.stage2_transform.models import TransformContext
from spartment.stage2_transform.standardize import (
    bool_text,
    days_between,
    format_date,
    format_period,
    format_timestamp,
    money,
    rate,
    surrogate,
)


def _reject(
    ctx: TransformContext,
    source_table: str,
    output_table: str,
    source_row: dict[str, Any],
    reason: str,
) -> None:
    mask: tuple[str, ...] = (
        ("phone",)
        if source_table in {"profiles", "tenant_assignments", "inquiries"}
        else ()
    )
    ctx.reject(
        source_table,
        output_table,
        source_row["_raw"],
        "C-13",
        reason,
        mask,
    )


def _demand_parent(
    ctx: TransformContext,
    source_table: str,
    output_table: str,
    source_row: dict[str, Any],
    parent_name: str,
) -> None:
    ctx.record_dangling_reference(source_table, output_table, source_row, parent_name)
    _reject(ctx, source_table, output_table, source_row, f"parent member unavailable: {parent_name}")


def _as_decimal(value: Any) -> Decimal:
    return Decimal(str(value)) if value is not None else Decimal("0")


def _as_date(value: Any) -> date | None:
    return date.fromisoformat(value) if value else None


def _overdue_bucket(days_overdue: int) -> str:
    if days_overdue <= 0:
        return "CURRENT"
    if days_overdue <= 30:
        return "D1_30"
    if days_overdue <= 60:
        return "D31_60"
    if days_overdue <= 90:
        return "D61_90"
    return "D90_PLUS"


def build_fact_assignment(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    tenant_keys: dict[str, str],
    room_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    rejected_assignments = {
        record.source_values.get("id")
        for record in ctx.rejections("tenant_assignments")
        if record.rule_id == "C-13"
    }
    for source in accepted["tenant_assignments"]:
        tenant_key = tenant_keys.get(source["id"])
        if tenant_key is None:
            if source["id"] not in rejected_assignments:
                _demand_parent(ctx, "tenant_assignments", "fact_assignment", source, "tenant")
            continue
        room_key = None
        if source.get("room_id") is not None:
            room_key = room_keys.get(source["room_id"])
            if room_key is None:
                _demand_parent(ctx, "tenant_assignments", "fact_assignment", source, "room")
                continue
        status = source.get("status") or "UNKNOWN"
        is_active = status == "ACTIVE"
        output = {
            "assignment_key": surrogate(ENTITY["fact_assignment"], source["id"]),
            "assignment_id": source["id"],
            "tenant_key": tenant_key,
            "room_key": room_key,
            "monthly_rent": money(source.get("monthly_rent")),
            "move_in": format_date(source.get("move_in")),
            "lease_end": format_date(source.get("lease_end")),
            "status": status,
            "is_active": bool_text(is_active),
            "tenure_days": days_between(source.get("move_in"), source.get("lease_end")),
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "date_corrected": bool_text(source.get("_date_corrected", False)),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "tenant_assignments", "fact_assignment", source, output)
        if finalized is None:
            continue
        rows.append(finalized)
        ctx.record_rule("B-01", "tenant_assignments", output_table="fact_assignment")
        ctx.record_rule("B-06", "tenant_assignments", output_table="fact_assignment")
        if not is_active:
            ctx.record_exclusion("X-01", "tenant_assignments", output_table="fact_assignment")
    return rows


def build_fact_billing(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    lease_outputs: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["bills"]:
        lease = lease_outputs.get(source.get("lease_id"))
        if lease is None:
            _demand_parent(ctx, "bills", "fact_billing", source, "lease")
            continue
        period = source.get("billing_period")
        due = source.get("due_date")
        if not isinstance(period, date) or not isinstance(due, date):
            _reject(ctx, "bills", "fact_billing", source, "billing period or due date is missing")
            continue
        rent = _as_decimal(source.get("rent_amount"))
        electricity = _as_decimal(source.get("electricity_amount"))
        water = _as_decimal(source.get("water_amount"))
        total = rent + electricity + water
        bill_type = source.get("bill_type") or "UNKNOWN"
        status = source.get("status") or "UNKNOWN"
        days_overdue = max((ctx.reference_date - due).days, 0)
        output = {
            "bill_key": surrogate(ENTITY["fact_billing"], source["id"]),
            "bill_id": source["id"],
            "lease_key": lease["lease_key"],
            "tenant_key": lease["tenant_key"],
            "room_key": lease["room_key"],
            "bill_type": bill_type,
            "billing_period_date": format_date(period),
            "billing_year": period.year,
            "billing_month": period.month,
            "billing_period": format_period(period),
            "due_date": format_date(due),
            "rent_amount": money(rent),
            "electricity_amount": money(electricity),
            "water_amount": money(water),
            "total_amount": money(total),
            "amount_paid": money(Decimal("0")),
            "balance": money(total),
            "status": status,
            "status_recomputed": "FALSE",
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "is_rent": bool_text(bill_type == "RENT"),
            "is_utility": bool_text(bill_type == "UTILITY"),
            "days_to_due": (due - period).days,
            "days_overdue": days_overdue,
            "is_overdue": bool_text(days_overdue > 0),
            "overdue_bucket": _overdue_bucket(days_overdue),
            "is_revenue_recognized": "FALSE",
            "due_date_corrected": bool_text(source.get("_due_date_corrected", False)),
            "collection_rate": None,
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "bills", "fact_billing", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "bills", output_table="fact_billing")
            ctx.record_rule("B-09", "bills", output_table="fact_billing")
            ctx.record_rule("B-10", "bills", output_table="fact_billing")
    return rows


def build_fact_payment(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    billing_outputs: dict[str, dict[str, Any]],
    correction_counts: dict[str, int],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["payments"]:
        bill = billing_outputs.get(source.get("bill_id"))
        if bill is None:
            _demand_parent(ctx, "payments", "fact_payment", source, "bill")
            continue
        payment_date = source.get("payment_date")
        if not isinstance(payment_date, date):
            _reject(ctx, "payments", "fact_payment", source, "payment date is missing")
            continue
        if payment_date > ctx.reference_date:
            ctx.record_exclusion("X-04", "payments", output_table="fact_payment")
            ctx.reject(
                "payments",
                "fact_payment",
                source["_raw"],
                "X-04",
                f"future-dated payment: {payment_date.isoformat()}",
            )
            continue
        correction_count = correction_counts.get(source["id"], 0)
        created = source.get("created_at")
        output = {
            "payment_key": surrogate(ENTITY["fact_payment"], source["id"]),
            "payment_id": source["id"],
            "bill_key": bill["bill_key"],
            "lease_key": bill["lease_key"],
            "tenant_key": bill["tenant_key"],
            "amount": money(source.get("amount")),
            "payment_date": format_date(payment_date),
            "payment_year": payment_date.year,
            "payment_month": payment_date.month,
            "reference_note": source.get("reference_note"),
            "payment_allocation_rank": 0,
            "is_oldest_payment": "FALSE",
            "correction_count": correction_count,
            "is_corrected": bool_text(correction_count > 0),
            "_created_at": created,
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "payments", "fact_payment", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "payments", output_table="fact_payment")
            ctx.record_rule("B-14", "payments", output_table="fact_payment")
    _assign_payment_ranks(rows)
    ctx.record_rule("B-13", "payments", len(rows), output_table="fact_payment")
    return rows


def _assign_payment_ranks(rows: list[dict[str, Any]]) -> None:
    by_bill: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_bill[row["bill_key"]].append(row)
    for bill_rows in by_bill.values():
        bill_rows.sort(
            key=lambda row: (
                row["payment_date"],
                row["_created_at"].timestamp() if isinstance(row["_created_at"], datetime) else 0,
                row["payment_id"],
            )
        )
        for index, row in enumerate(bill_rows, start=1):
            row["payment_allocation_rank"] = index
            row["is_oldest_payment"] = bool_text(index == 1)
    for row in rows:
        row.pop("_created_at", None)


def build_fact_payment_correction(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    payment_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["payment_corrections"]:
        payment_key = payment_keys.get(source.get("payment_id"))
        if payment_key is None:
            _demand_parent(
                ctx, "payment_corrections", "fact_payment_correction", source, "payment"
            )
            continue
        previous, previous_valid = source.get("previous_values"), source.get(
            "_previous_values_valid", True
        )
        corrected, corrected_valid = source.get("corrected_values"), source.get(
            "_corrected_values_valid", True
        )
        output = {
            "correction_key": surrogate(ENTITY["fact_payment_correction"], source["id"]),
            "correction_id": source["id"],
            "payment_key": payment_key,
            "payment_id": source.get("payment_id"),
            "corrected_by": source.get("corrected_by"),
            "reason": source.get("reason"),
            "previous_values": previous,
            "corrected_values": corrected,
            "json_valid": bool_text(bool(previous_valid) and bool(corrected_valid)),
            "created_at": format_timestamp(source.get("created_at")),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(
            ctx, "payment_corrections", "fact_payment_correction", source, output
        )
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "payment_corrections", output_table="fact_payment_correction")
            ctx.record_exclusion("X-03", "payment_corrections", output_table="fact_payment_correction")
    return rows


def build_fact_inquiry(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    room_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["inquiries"]:
        room_key = None
        if source.get("room_id") is not None:
            room_key = room_keys.get(source["room_id"])
            if room_key is None:
                _demand_parent(ctx, "inquiries", "fact_inquiry", source, "room")
                continue
        created = source.get("created_at")
        age_days = (
            (ctx.reference_date - created.date()).days
            if isinstance(created, datetime)
            else 0
        )
        status = source.get("status") or "UNKNOWN"
        output = {
            "inquiry_key": surrogate(ENTITY["fact_inquiry"], source["id"]),
            "inquiry_id": source["id"],
            "room_key": room_key,
            "full_name": source.get("full_name"),
            "email": source.get("email"),
            "phone": source.get("phone"),
            "preferred_room": source.get("preferred_room"),
            "move_in_date": format_date(source.get("move_in_date")),
            "status": status,
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "created_at": format_timestamp(created),
            "age_days": age_days,
            "is_open": bool_text(status in {"NEW", "CONTACTED"}),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "inquiries", "fact_inquiry", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "inquiries", output_table="fact_inquiry")
            ctx.record_rule("B-16", "inquiries", output_table="fact_inquiry")
            if status == "RESOLVED":
                ctx.record_exclusion("X-05", "inquiries", output_table="fact_inquiry")
    return rows


def build_fact_tenant_request(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    tenant_keys: dict[str, str],
    room_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["tenant_requests"]:
        tenant_key = tenant_keys.get(source.get("tenant_id"))
        if tenant_key is None:
            _demand_parent(ctx, "tenant_requests", "fact_tenant_request", source, "tenant")
            continue
        room_key = None
        if source.get("room_id") is not None:
            room_key = room_keys.get(source["room_id"])
            if room_key is None:
                _demand_parent(ctx, "tenant_requests", "fact_tenant_request", source, "room")
                continue
        status = source.get("status") or "UNKNOWN"
        created = source.get("created_at")
        updated = source.get("updated_at")
        resolution_days = None
        if status == "RESOLVED" and isinstance(created, datetime) and isinstance(updated, datetime):
            resolution_days = (updated.date() - created.date()).days
        output = {
            "request_key": surrogate(ENTITY["fact_tenant_request"], source["id"]),
            "request_id": source["id"],
            "tenant_key": tenant_key,
            "room_key": room_key,
            "kind": source.get("kind") or "UNKNOWN",
            "subject": source.get("subject"),
            "priority": source.get("priority") or "UNKNOWN",
            "status": status,
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "created_at": format_timestamp(created),
            "updated_at": format_timestamp(updated),
            "resolution_days": resolution_days,
            "is_open": bool_text(status != "RESOLVED"),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "tenant_requests", "fact_tenant_request", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "tenant_requests", output_table="fact_tenant_request")
            ctx.record_rule("B-17", "tenant_requests", output_table="fact_tenant_request")
            if status == "RESOLVED":
                ctx.record_exclusion("X-05", "tenant_requests", output_table="fact_tenant_request")
    return rows


def build_fact_occupancy_event(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    tenant_keys: dict[str, str],
    lease_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["tenant_occupancy_events"]:
        tenant_key = tenant_keys.get(source.get("tenant_id"))
        previous_key = lease_keys.get(source.get("previous_lease_id"))
        new_lease_id = source.get("new_lease_id")
        new_key = lease_keys.get(new_lease_id) if new_lease_id else None
        actor_key = tenant_keys.get(source.get("performed_by"))
        if tenant_key is None:
            _demand_parent(
                ctx, "tenant_occupancy_events", "fact_occupancy_event", source, "tenant"
            )
            continue
        if previous_key is None:
            _demand_parent(
                ctx, "tenant_occupancy_events", "fact_occupancy_event", source, "previous lease"
            )
            continue
        if new_lease_id is not None and new_key is None:
            _demand_parent(
                ctx, "tenant_occupancy_events", "fact_occupancy_event", source, "new lease"
            )
            continue
        if actor_key is None:
            _demand_parent(
                ctx, "tenant_occupancy_events", "fact_occupancy_event", source, "actor"
            )
            continue
        if new_lease_id is None:
            event_type = "MOVE_OUT"
        elif source.get("previous_lease_id") != new_lease_id:
            event_type = "TRANSFER"
        else:
            event_type = "UNKNOWN"
        details, details_valid = source.get("details"), source.get("_details_valid", True)
        output = {
            "event_key": surrogate(ENTITY["fact_occupancy_event"], source["id"]),
            "event_id": source["id"],
            "tenant_key": tenant_key,
            "previous_lease_key": previous_key,
            "new_lease_key": new_key,
            "performed_by_tenant_key": actor_key,
            "event_type": event_type,
            "details": details,
            "json_valid": bool_text(bool(details_valid)),
            "created_at": format_timestamp(source.get("created_at")),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(
            ctx, "tenant_occupancy_events", "fact_occupancy_event", source, output
        )
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "tenant_occupancy_events", output_table="fact_occupancy_event")
            ctx.record_rule("B-18", "tenant_occupancy_events", output_table="fact_occupancy_event")
    return rows


def build_fact_notification(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    tenant_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["notifications"]:
        tenant_key = tenant_keys.get(source.get("tenant_id"))
        if tenant_key is None:
            _demand_parent(ctx, "notifications", "fact_notification", source, "tenant")
            continue
        output = {
            "notification_key": surrogate(ENTITY["fact_notification"], source["id"]),
            "notification_id": source["id"],
            "tenant_key": tenant_key,
            "kind": source.get("kind") or "UNKNOWN",
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "title": source.get("title"),
            "source_id": source.get("source_id"),
            "is_read": (
                bool_text(source.get("is_read"))
                if source.get("is_read") is not None
                else None
            ),
            "created_at": format_timestamp(source.get("created_at")),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "notifications", "fact_notification", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "notifications", output_table="fact_notification")
            ctx.record_rule("B-19", "notifications", output_table="fact_notification")
    return rows


def enrich_billing(
    fact_billing: list[dict[str, Any]],
    fact_payment: list[dict[str, Any]],
    ctx: TransformContext,
) -> None:
    paid_by_bill: dict[str, Decimal] = defaultdict(lambda: Decimal("0"))
    for payment in fact_payment:
        paid_by_bill[payment["bill_key"]] += _as_decimal(payment["amount"])
    for bill in fact_billing:
        total = _as_decimal(bill["total_amount"])
        paid = paid_by_bill.get(bill["bill_key"], Decimal("0"))
        balance = max(total - paid, Decimal("0"))
        if balance == 0:
            status = "PAID"
        elif paid > 0:
            status = "PARTIAL"
        else:
            status = "PENDING"
        bill["amount_paid"] = money(paid)
        bill["balance"] = money(balance)
        bill["status_recomputed"] = bool_text(status != bill["status"])
        bill["status"] = status
        bill["collection_rate"] = rate(paid / total) if total > 0 else None
        bill["is_revenue_recognized"] = bool_text(status == "PAID")
        due = _as_date(bill["due_date"])
        days_overdue = max((ctx.reference_date - due).days, 0) if due else 0
        bill["days_overdue"] = days_overdue
        bill["is_overdue"] = bool_text(days_overdue > 0)
        bill["overdue_bucket"] = _overdue_bucket(days_overdue)
        ctx.record_rule("B-11", "bills", output_table="fact_billing")
        ctx.record_rule("B-12", "bills", output_table="fact_billing")
        ctx.record_rule("B-21", "bills", output_table="fact_billing")
        if status != "PAID" or balance > 0:
            ctx.record_exclusion("X-02", "bills", output_table="fact_billing")

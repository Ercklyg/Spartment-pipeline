"""Dimension builders for the Stage 2 star schema."""

from __future__ import annotations

import calendar
from collections import defaultdict
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
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


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(Decimal(str(value)))
    except (InvalidOperation, ValueError):
        return None


def _timestamp_date(value: Any) -> date | None:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    return None


def _demand_parent(
    ctx: TransformContext,
    source_table: str,
    output_table: str,
    source_row: dict[str, Any],
    parent_name: str,
) -> None:
    ctx.record_dangling_reference(source_table, output_table, source_row, parent_name)
    ctx.reject(
        source_table,
        output_table,
        source_row["_raw"],
        "C-13",
        f"parent dimension member is unavailable: {parent_name}",
    )


def build_dim_date(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
) -> list[dict[str, Any]]:
    dates: set[date] = set(ctx.source_dates)
    for rows in accepted.values():
        for row in rows:
            for value in row.values():
                if isinstance(value, datetime):
                    dates.add(value.date())
                elif isinstance(value, date):
                    dates.add(value)
    if not dates:
        dates = {ctx.reference_date}
    start = min(dates)
    end = max(dates)
    if end < start:
        start, end = end, start

    rows: list[dict[str, Any]] = []
    current = start
    while current <= end:
        iso = current.isocalendar()
        rows.append(
            {
                "date_key": current.strftime("%Y%m%d"),
                "date": format_date(current),
                "year": current.year,
                "quarter": (current.month - 1) // 3 + 1,
                "month": current.month,
                "month_name": calendar.month_name[current.month],
                "week": iso.week,
                "day_of_month": current.day,
                "day_of_week": current.isoweekday(),
                "day_name": calendar.day_name[current.isoweekday() - 1],
                "is_weekend": bool_text(current.isoweekday() >= 6),
                "billing_period": format_period(current),
            }
        )
        current += timedelta(days=1)
    return rows


def build_dim_building(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    include_room_rollups: bool = True,
) -> list[dict[str, Any]]:
    rooms_by_building: dict[str, list[dict[str, Any]]] = defaultdict(list)
    if include_room_rollups:
        for room in accepted["rooms"]:
            rooms_by_building[str(room.get("building_id"))].append(room)

    rows: list[dict[str, Any]] = []
    for source in accepted["buildings"]:
        building_id = source["id"]
        rooms = rooms_by_building.get(building_id, [])
        occupied = sum(1 for room in rooms if room.get("status") == "OCCUPIED")
        available = sum(1 for room in rooms if room.get("status") == "AVAILABLE")
        room_count = len(rooms)
        vacancy = sum(
            (
                (room.get("monthly_rate") or Decimal("0"))
                for room in rooms
                if room.get("status") == "AVAILABLE"
            ),
            Decimal("0"),
        )
        output = {
            "building_key": surrogate(ENTITY["dim_building"], building_id),
            "building_id": building_id,
            "name": source.get("name"),
            "address": source.get("address"),
            "room_count": room_count,
            "occupied_rooms": occupied,
            "available_rooms": available,
            "occupancy_rate": (
                rate(Decimal(occupied) / Decimal(room_count)) if room_count else None
            ),
            "vacancy_gap": money(vacancy),
            "created_at": format_timestamp(source.get("created_at")),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "buildings", "dim_building", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "buildings", output_table="dim_building")
            ctx.record_rule("B-08", "buildings", output_table="dim_building")
    return rows


def apply_building_rollups(
    dim_building: list[dict[str, Any]],
    dim_room: list[dict[str, Any]],
) -> None:
    rooms_by_building: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for room in dim_room:
        rooms_by_building[room["building_key"]].append(room)
    for building in dim_building:
        rooms = rooms_by_building.get(building["building_key"], [])
        occupied = sum(1 for room in rooms if room.get("status") == "OCCUPIED")
        available = sum(1 for room in rooms if room.get("status") == "AVAILABLE")
        room_count = len(rooms)
        vacancy = sum(
            (
                Decimal(str(room.get("monthly_rate") or "0"))
                for room in rooms
                if room.get("status") == "AVAILABLE"
            ),
            Decimal("0"),
        )
        building["room_count"] = room_count
        building["occupied_rooms"] = occupied
        building["available_rooms"] = available
        building["occupancy_rate"] = (
            rate(Decimal(occupied) / Decimal(room_count)) if room_count else None
        )
        building["vacancy_gap"] = money(vacancy)


def build_dim_room(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    building_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["rooms"]:
        building_id = source.get("building_id")
        building_key = building_keys.get(building_id)
        if building_key is None:
            _demand_parent(ctx, "rooms", "dim_room", source, f"building {building_id}")
            continue
        status = source.get("status") or "UNKNOWN"
        size = source.get("size_sqm")
        output = {
            "room_key": surrogate(ENTITY["dim_room"], source["id"]),
            "room_id": source["id"],
            "building_key": building_key,
            "building_id": building_id,
            "room_number": source.get("room_number"),
            "floor": _int_or_none(source.get("floor")),
            "status": status,
            "status_raw": source.get("_status_raw"),
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "size_sqm": money(size),
            "size_out_of_bounds": bool_text(
                size is not None and (size < Decimal("8") or size > Decimal("14"))
            ),
            "capacity": source.get("capacity"),
            "monthly_rate": money(source.get("monthly_rate")),
            "description": source.get("description"),
            "is_available": bool_text(status == "AVAILABLE"),
            "is_occupied": bool_text(status == "OCCUPIED"),
            "is_under_maintenance": bool_text(status == "MAINTENANCE"),
            "created_at": format_timestamp(source.get("created_at")),
            "updated_at": format_timestamp(source.get("updated_at")),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "rooms", "dim_room", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "rooms", output_table="dim_room")
            ctx.record_rule("B-07", "rooms", output_table="dim_room")
            ctx.record_rule("B-20", "rooms", output_table="dim_room")
    return rows


def _assignment_rank(row: dict[str, Any]) -> tuple[int, float, str]:
    active = 1 if row.get("status") == "ACTIVE" else 0
    created = row.get("created_at")
    timestamp = created.timestamp() if isinstance(created, datetime) else 0.0
    return active, timestamp, str(row.get("id", ""))


def build_dim_tenant(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
) -> list[dict[str, Any]]:
    profiles = {row["id"]: row for row in accepted["profiles"]}
    assignments_by_id: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in accepted["tenant_assignments"]:
        assignments_by_id[str(row["id"])].append(row)

    active_assignments = {
        str(row["id"])
        for row in accepted["tenant_assignments"]
        if row.get("status") == "ACTIVE"
    }
    active_leases = {
        str(row.get("tenant_id"))
        for row in accepted["leases"]
        if row.get("status") == "ACTIVE"
    }

    identities = sorted(set(profiles) | set(assignments_by_id))
    rows: list[dict[str, Any]] = []
    for tenant_id in identities:
        profile = profiles.get(tenant_id)
        assignments = assignments_by_id.get(tenant_id, [])
        assignment = (
            max(assignments, key=_assignment_rank) if assignments else None
        )
        identity_source = (
            "BOTH" if profile and assignment else "PROFILE" if profile else "ASSIGNMENT"
        )
        role = profile.get("role") if profile else "UNKNOWN"
        role_unmapped = bool(profile.get("_unmapped")) if profile else False
        assignment_unmapped = bool(assignment.get("_unmapped")) if assignment else False
        full_name = (
            profile.get("full_name")
            if profile and profile.get("full_name")
            else assignment.get("full_name") if assignment else None
        )
        phone = (
            profile.get("phone")
            if profile is not None
            else assignment.get("phone") if assignment else None
        )
        email = assignment.get("email") if assignment else None
        created_values = [
            value
            for value in (
                profile.get("created_at") if profile else None,
                assignment.get("created_at") if assignment else None,
            )
            if isinstance(value, datetime)
        ]
        created_at = min(created_values) if created_values else None
        is_tenant = role == "TENANT" or identity_source == "ASSIGNMENT"
        is_staff = role == "ADMIN" and identity_source != "ASSIGNMENT"
        active_flag = tenant_id in active_assignments or tenant_id in active_leases

        output = {
            "tenant_key": surrogate(ENTITY["dim_tenant"], tenant_id),
            "tenant_id": tenant_id,
            "full_name": full_name,
            "phone": phone,
            "role": role,
            "is_tenant": bool_text(is_tenant),
            "is_staff": bool_text(is_staff),
            "email": email,
            "current_assignment_id": assignment.get("id") if assignment else None,
            "monthly_rent": money(assignment.get("monthly_rent")) if assignment else None,
            "move_in": format_date(assignment.get("move_in")) if assignment else None,
            "lease_end": format_date(assignment.get("lease_end")) if assignment else None,
            "assignment_status": assignment.get("status") if assignment else None,
            "emergency_name": assignment.get("emergency_name") if assignment else None,
            "emergency_phone": assignment.get("emergency_phone") if assignment else None,
            "relationship": assignment.get("relationship") if assignment else None,
            "identity_source": identity_source,
            "active_flag": bool_text(active_flag),
            "outstanding_bill_count": 0,
            "is_flagged_tenant": "FALSE",
            "code_unmapped": bool_text(role_unmapped or assignment_unmapped),
            "created_at": format_timestamp(created_at),
            "transformation_run_id": ctx.run_id,
        }
        source_table = "profiles" if profile is not None else "tenant_assignments"
        source = profile if profile is not None else assignment
        finalized = finalize(ctx, source_table, "dim_tenant", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", source_table, output_table="dim_tenant")
            ctx.record_rule("B-03", source_table, output_table="dim_tenant")
            ctx.record_rule("B-04", source_table, output_table="dim_tenant")
            if is_tenant and not active_flag:
                ctx.record_exclusion("X-01", source_table, output_table="dim_tenant")
            if is_staff:
                ctx.record_exclusion("X-06", source_table, output_table="dim_tenant")
    return rows


def build_dim_lease(
    accepted: dict[str, list[dict[str, Any]]],
    ctx: TransformContext,
    tenant_keys: dict[str, str],
    room_keys: dict[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for source in accepted["leases"]:
        tenant_key = tenant_keys.get(source.get("tenant_id"))
        room_key = room_keys.get(source.get("room_id"))
        if tenant_key is None:
            _demand_parent(ctx, "leases", "dim_lease", source, f"tenant {source.get('tenant_id')}")
            continue
        if room_key is None:
            _demand_parent(ctx, "leases", "dim_lease", source, f"room {source.get('room_id')}")
            continue
        end_date = source.get("lease_end_date")
        status = source.get("status") or "UNKNOWN"
        output = {
            "lease_key": surrogate(ENTITY["dim_lease"], source["id"]),
            "lease_id": source["id"],
            "tenant_key": tenant_key,
            "tenant_id": source.get("tenant_id"),
            "room_key": room_key,
            "room_id": source.get("room_id"),
            "move_in_date": format_date(source.get("move_in_date")),
            "lease_end_date": format_date(end_date),
            "status": status,
            "is_active": bool_text(status == "ACTIVE"),
            "is_open_ended": bool_text(end_date is None),
            "lease_days": days_between(source.get("move_in_date"), end_date),
            "code_unmapped": bool_text(source.get("_unmapped", False)),
            "date_corrected": bool_text(source.get("_date_corrected", False)),
            "transformation_run_id": ctx.run_id,
        }
        finalized = finalize(ctx, "leases", "dim_lease", source, output)
        if finalized is not None:
            rows.append(finalized)
            ctx.record_rule("B-01", "leases", output_table="dim_lease")
            ctx.record_rule("B-05", "leases", output_table="dim_lease")
            if status != "ACTIVE":
                ctx.record_exclusion("X-01", "leases", output_table="dim_lease")
    return rows


def apply_tenant_rollups(
    dim_tenant: list[dict[str, Any]],
    fact_billing: list[dict[str, Any]],
    ctx: TransformContext,
) -> None:
    counts: dict[str, int] = defaultdict(int)
    for bill in fact_billing:
        if (
            bill.get("status") in {"PENDING", "PARTIAL"}
            and bill.get("due_date") is not None
            and bill["due_date"] < ctx.reference_date.isoformat()
        ):
            counts[bill["tenant_key"]] += 1
    for tenant in dim_tenant:
        count = counts.get(tenant["tenant_key"], 0)
        tenant["outstanding_bill_count"] = count
        tenant["is_flagged_tenant"] = bool_text(count >= 3)
        source = (
            "profiles"
            if tenant.get("identity_source") in {"PROFILE", "BOTH"}
            else "tenant_assignments"
        )
        ctx.record_rule("B-15", source, output_table="dim_tenant")

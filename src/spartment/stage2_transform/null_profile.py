"""Separate contract-permitted absence from optional-field quality degradation."""
from __future__ import annotations

from typing import Any

OPTIONAL_NULL_PROFILE_VERSION = 2
OPTIONAL_COLUMNS: dict[str, tuple[str, ...]] = {
    "dim_tenant": ("phone", "email", "lease_end"),
    "dim_lease": ("lease_end_date",),
    "fact_assignment": ("room_key", "lease_end"),
    "fact_payment": ("reference_note",),
    "fact_inquiry": ("room_key", "phone"),
    "fact_tenant_request": ("room_key",),
}

# C-03/B-05/B-06 and the published nullable-FK contract explicitly allow these
# absences. Do not manufacture an end date or a room merely to improve a rate.
EXPECTED_ABSENCE = {
    ("dim_tenant", "lease_end"),
    ("dim_lease", "lease_end_date"),
    ("fact_assignment", "lease_end"),
    ("fact_inquiry", "room_key"),
}


def expected_absence(table: str, column: str, row: dict[str, Any]) -> bool:
    if (table, column) in EXPECTED_ABSENCE:
        return True
    if (table, column) == ("fact_assignment", "room_key"):
        return row.get("status") == "ENDED"
    if (table, column) == ("fact_tenant_request", "room_key"):
        return row.get("kind") == "OTHER"
    return False


def optional_null_profile(table: str, rows: list[dict[str, Any]]) -> dict[str, dict]:
    result = {}
    for column in OPTIONAL_COLUMNS.get(table, ()):
        null_rows = [r for r in rows if r.get(column) is None or r.get(column) == ""]
        expected = sum(expected_absence(table, column, r) for r in null_rows)
        eligible = len(rows) - expected
        unexpected = len(null_rows) - expected
        result[column] = {
            "rows": len(rows), "nulls": len(null_rows), "expected_nulls": expected,
            "eligible_rows": eligible, "unexpected_nulls": unexpected,
            "raw_null_rate": round(len(null_rows) / len(rows), 6) if rows else 0.0,
            "unexpected_null_rate": round(unexpected / eligible, 6) if eligible else 0.0,
        }
    return result

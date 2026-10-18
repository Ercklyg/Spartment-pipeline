"""The extraction contract: tables, columns, identifiers and domain rules.

The required column lists follow the selection documented in Docs/extraction.md,
extended with the additional operational tables the analysis depends on. Every
column list here was verified against the live PostgREST schema.

Optional entries are selected only when the live schema exposes them.
"""

from __future__ import annotations

from dataclasses import dataclass

EXTRA_COLUMNS: dict[str, tuple[str, ...]] = {
    "rooms": ("description",),
    "tenant_assignments": ("emergency_name", "emergency_phone", "relationship"),
    "inquiries": ("message",),
    "tenant_requests": ("message", "admin_note"),
    "tenant_occupancy_events": ("details",),
}


@dataclass(frozen=True)
class TableSpec:
    name: str
    columns: tuple[str, ...]
    optional: tuple[str, ...] = ()

    def optional_for(self, include_extra: bool) -> tuple[str, ...]:
        extras = EXTRA_COLUMNS.get(self.name, ()) if include_extra else ()
        merged = dict.fromkeys((*self.optional, *extras))
        return tuple(name for name in merged if name not in self.columns)


TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        "profiles",
        ("id", "full_name", "phone", "role", "created_at"),
    ),
    TableSpec(
        "buildings",
        ("id", "name", "address", "created_at"),
    ),
    TableSpec(
        "rooms",
        (
            "id",
            "building_id",
            "room_number",
            "floor",
            "status",
            "size_sqm",
            "capacity",
            "monthly_rate",
            "created_at",
            "updated_at",
        ),
    ),
    TableSpec(
        "leases",
        ("id", "tenant_id", "room_id", "move_in_date", "lease_end_date", "status"),
    ),
    TableSpec(
        "tenant_assignments",
        (
            "id",
            "room_id",
            "full_name",
            "email",
            "phone",
            "monthly_rent",
            "move_in",
            "lease_end",
            "status",
            "created_at",
        ),
    ),
    TableSpec(
        "bills",
        (
            "id",
            "lease_id",
            "bill_type",
            "billing_period",
            "due_date",
            "rent_amount",
            "electricity_amount",
            "water_amount",
            "status",
            "created_at",
        ),
    ),
    TableSpec(
        "payments",
        ("id", "bill_id", "amount", "payment_date", "reference_note", "created_at"),
    ),
    TableSpec(
        "payment_corrections",
        (
            "id",
            "payment_id",
            "corrected_by",
            "reason",
            "previous_values",
            "corrected_values",
            "created_at",
        ),
    ),
    TableSpec(
        "inquiries",
        (
            "id",
            "full_name",
            "email",
            "phone",
            "preferred_room",
            "move_in_date",
            "status",
            "created_at",
        ),
        optional=("room_id",),
    ),
    TableSpec(
        "tenant_requests",
        (
            "id",
            "tenant_id",
            "room_id",
            "kind",
            "subject",
            "priority",
            "status",
            "created_at",
            "updated_at",
        ),
    ),
    TableSpec(
        "tenant_occupancy_events",
        (
            "id",
            "tenant_id",
            "previous_lease_id",
            "new_lease_id",
            "performed_by",
            "created_at",
        ),
    ),
    TableSpec(
        "notifications",
        ("id", "tenant_id", "title", "message", "kind", "source_id", "is_read", "created_at"),
    ),
)

TABLE_NAMES: tuple[str, ...] = tuple(spec.name for spec in TABLES)
SPECS_BY_NAME: dict[str, TableSpec] = {spec.name: spec for spec in TABLES}


def select_columns(
    spec: TableSpec,
    available: object,
    include_extra: bool = True,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """Resolve the columns to request for a table.

    Returns the selected columns in contract order and the required columns
    that the live schema does not expose.
    """
    present = set(available)
    selected = tuple(name for name in spec.columns if name in present)
    missing = tuple(name for name in spec.columns if name not in present)
    if include_extra:
        selected += tuple(
            name
            for name in spec.optional_for(include_extra=True)
            if name in present and name not in selected
        )
    return selected, missing


REQUIRED_IDENTIFIERS: dict[str, tuple[str, ...]] = {
    "profiles": ("id", "role"),
    "buildings": ("id", "name"),
    "rooms": ("id", "status"),
    "leases": ("id", "tenant_id", "move_in_date"),
    "tenant_assignments": ("id", "full_name", "email", "phone", "monthly_rent", "move_in", "status"),
    "bills": ("id", "billing_period", "due_date"),
    "payments": ("id", "bill_id", "amount", "payment_date"),
    "payment_corrections": ("id", "payment_id", "reason", "created_at"),
    "inquiries": ("id", "email", "status"),
    "tenant_requests": ("id", "tenant_id", "kind", "subject", "priority", "status"),
    "tenant_occupancy_events": ("id", "tenant_id", "previous_lease_id", "performed_by", "created_at"),
    "notifications": ("id", "tenant_id", "title", "created_at"),
}


@dataclass(frozen=True)
class Relationship:
    child_table: str
    child_column: str
    parent_table: str
    parent_column: str = "id"
    available: bool = True


RELATIONSHIPS: tuple[Relationship, ...] = (
    Relationship("rooms", "building_id", "buildings"),
    Relationship("leases", "tenant_id", "profiles"),
    Relationship("leases", "room_id", "rooms"),
    Relationship("tenant_assignments", "room_id", "rooms"),
    Relationship("tenant_assignments", "id", "auth.users", available=False),
    Relationship("bills", "lease_id", "leases"),
    Relationship("payments", "bill_id", "bills"),
    Relationship("payment_corrections", "payment_id", "payments"),
    Relationship("payment_corrections", "corrected_by", "auth.users", available=False),
    Relationship("inquiries", "room_id", "rooms"),
    Relationship("tenant_requests", "tenant_id", "profiles"),
    Relationship("tenant_requests", "room_id", "rooms"),
    Relationship("tenant_occupancy_events", "tenant_id", "profiles"),
    Relationship("tenant_occupancy_events", "previous_lease_id", "leases"),
    Relationship("tenant_occupancy_events", "new_lease_id", "leases"),
    Relationship("tenant_occupancy_events", "performed_by", "profiles"),
    Relationship("notifications", "tenant_id", "profiles"),
)

ENUM_COLUMNS: tuple[tuple[str, str], ...] = (
    ("profiles", "role"),
    ("rooms", "status"),
    ("leases", "status"),
    ("bills", "status"),
)

MONETARY_COLUMNS: dict[str, tuple[str, ...]] = {
    "rooms": ("monthly_rate",),
    "tenant_assignments": ("monthly_rent",),
    "bills": ("rent_amount", "electricity_amount", "water_amount"),
    "payments": ("amount",),
}

DATE_COLUMNS: dict[str, tuple[str, ...]] = {
    "leases": ("move_in_date", "lease_end_date"),
    "tenant_assignments": ("move_in", "lease_end"),
    "bills": ("billing_period", "due_date"),
    "payments": ("payment_date",),
    "inquiries": ("move_in_date",),
}

DATE_ORDERING_RULES: tuple[tuple[str, str, str, str], ...] = (
    ("tenant_assignments", "lease_end", "move_in", "lease_end >= move_in"),
    ("leases", "lease_end_date", "move_in_date", "lease_end_date >= move_in_date"),
)

NUMERIC_BOUNDS: dict[str, dict[str, tuple[float, float]]] = {
    "rooms": {"size_sqm": (8.0, 14.0)},
}

ASSIGNMENT_ROOM_INVARIANT: tuple[str, str] = ("tenant_assignments", "room_id")


def standardized_mapping() -> dict[str, dict[str, dict[str, str]]]:
    """Source to standardized column mapping, extraction.md section 6.4.

    Every extracted column is carried through unchanged. Normalization of
    values is deliberately deferred to the transformation stage, so the mapping
    type is Direct for all columns and no renaming occurs.
    """
    return {
        spec.name: {
            column: {
                "source_column": column,
                "standardized_column": column,
                "mapping_type": "Direct",
            }
            for column in (*spec.columns, *spec.optional_for(include_extra=True))
        }
        for spec in TABLES
    }
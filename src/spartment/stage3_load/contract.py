"""Storage contract: reuse Stage 2's public output contract without transforming."""
from __future__ import annotations

import hashlib
import json

from spartment.stage2_transform.contract import (
    CODE_DOMAINS, FK_COLUMNS, NATURAL_KEY, NON_NEGATIVE, OUTPUT_COLUMNS,
    OUTPUT_TABLES, OUTPUT_TYPES, REQUIRED_NOT_NULL, RULES_VERSION, SURROGATE_KEY,
)

SCHEMA_VERSION = "1.0.0"
LOCK_ID = 734215830
LINEAGE = ("load_run_id", "load_batch_id", "loaded_at")
FAMILIES = {
    "surrogate": "character varying(16)", "uuid": "uuid", "int": "integer",
    "money": "numeric(18,2)", "rate": "numeric", "area": "numeric",
    "bool": "boolean", "date": "date", "timestamp": "timestamp with time zone",
    "text": "text", "code": "text",
}


def primary_key(table: str) -> str:
    return "date_key" if table == "dim_date" else SURROGATE_KEY[table]


def sql_type(table: str, column: str) -> str:
    if (table, column) == ("dim_date", "date_key"):
        return "character varying(8)"
    return FAMILIES[OUTPUT_TYPES[table][column]]


def fingerprint() -> str:
    value = [SCHEMA_VERSION, RULES_VERSION, OUTPUT_COLUMNS, OUTPUT_TYPES,
             REQUIRED_NOT_NULL, FK_COLUMNS, NON_NEGATIVE,
             {t: {c: sorted(v) for c, v in d.items()} for t, d in CODE_DOMAINS.items()}]
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()

"""Freeze and verify the exact Stage 2 handover; never read the source system."""
from __future__ import annotations

import csv
import hashlib
import json
import re
import shutil
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from uuid import UUID

from .contract import (CODE_DOMAINS, FK_COLUMNS, NATURAL_KEY, NON_NEGATIVE,
                       OUTPUT_COLUMNS, OUTPUT_TABLES, OUTPUT_TYPES,
                       REQUIRED_NOT_NULL, RULES_VERSION, primary_key)
from .models import LoadError, Package


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise ValueError()
        return value
    except (OSError, ValueError) as exc:
        raise LoadError("INPUT_METADATA", f"Missing or invalid metadata: {path.name}") from exc


def safe_path(root: Path, value: str) -> Path:
    from spartment.common.config import REPO_ROOT
    path = (root / value).resolve()
    # Stage 2 paths inside the repository are relative to REPO_ROOT even when
    # a demonstration has a narrower artifact root such as loaded/demo.
    if not path.is_file() and not Path(value).is_absolute():
        path = (REPO_ROOT / value).resolve()
    if not path.is_relative_to(root.resolve()):
        raise LoadError("INPUT_PATH", "Input artifact must remain inside the project root")
    return path


def parse_value(table: str, column: str, value: str):
    family = OUTPUT_TYPES[table][column]
    if family in {"text", "code"}:
        return value
    if value == "":
        if column in REQUIRED_NOT_NULL[table]:
            raise ValueError("required value is empty")
        return None
    if family == "surrogate":
        if not re.fullmatch(r"[0-9a-f]{16}", value):
            raise ValueError("invalid surrogate")
        return value
    if family == "uuid":
        return UUID(value)
    if family == "int":
        if not re.fullmatch(r"-?\d+", value):
            raise ValueError("invalid integer")
        result = int(value)
        if not -(2**31) <= result < 2**31:
            raise ValueError("integer overflow")
        return result
    if family in {"money", "rate", "area"}:
        result = Decimal(value)
        if not result.is_finite():
            raise ValueError("nonfinite decimal")
        if family == "money" and (abs(result) >= Decimal("1e16") or result != result.quantize(Decimal("0.01"))):
            raise ValueError("money precision loss or overflow")
        return result
    if family == "bool":
        if value not in {"TRUE", "FALSE", "true", "false"}:
            raise ValueError("invalid boolean")
        return value.lower() == "true"
    if family == "date":
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
            raise ValueError("invalid date format")
        return date.fromisoformat(value)
    if family == "timestamp":
        result = datetime.fromisoformat(value)
        if result.tzinfo is None:
            raise ValueError("timestamp must include offset")
        return result
    raise ValueError("unsupported type")


def freeze(root: Path, handover_path: Path, folder: Path) -> Package:
    root = root.resolve()
    folder.mkdir(parents=True, exist_ok=False)
    shutil.copyfile(handover_path, folder / "handover.json")
    h = read_json(folder / "handover.json")
    run = h.get("run_id", "")
    if not re.fullmatch(r"spartment-transform-[\w-]+", run):
        raise LoadError("INPUT_ID", "Invalid transformation run ID")
    if h.get("acceptance_decision", {}).get("decision") != "APPROVED" or h.get("released_for_load") is not True:
        raise LoadError("UPSTREAM_APPROVAL", "An APPROVED and released transformation is required")
    if h.get("validation_result") != "PASS" or h.get("extraction_validation_summary") != "APPROVED":
        raise LoadError("UPSTREAM_VALIDATION", "Upstream validation must pass")
    if h.get("rules_version") != RULES_VERSION:
        raise LoadError("CONTRACT_VERSION", "Unsupported transformation rules version")
    for field in ("batch_id", "parent_run_id", "batch_window_start", "batch_window_end"):
        if not isinstance(h.get(field), str) or not h[field]:
            raise LoadError("INPUT_METADATA", f"Missing {field}")
    for field in ("processing_period", "reference_date"):
        try:
            date.fromisoformat(h[field])
        except (KeyError, TypeError, ValueError) as exc:
            raise LoadError("INPUT_METADATA", f"Invalid {field}") from exc
    paths = {
        "validation": root / "transformed" / "_validation" / f"{run}.json",
        "summary": root / "logs" / "spartment-transformation" / h["processing_period"] / f"{run}.json",
    }
    meta = {}
    for name, path in paths.items():
        shutil.copyfile(path, folder / f"{name}.json")
        meta[name] = read_json(folder / f"{name}.json")
    v, summary = meta["validation"], meta["summary"]
    if (v.get("pipeline_run_id") != run or v.get("parent_run_id") != h["parent_run_id"]
            or v.get("decision", {}).get("decision") != "APPROVED"
            or v.get("validation", {}).get("validation_result") != "PASS"
            or summary.get("run_id") != run or summary.get("parent_run_id") != h["parent_run_id"]
            or summary.get("decision", {}).get("decision") != "APPROVED"
            or summary.get("validation_result") != "PASS"):
        raise LoadError("INPUT_LINEAGE", "Transformation evidence and handover disagree")
    for field in ("batch_id", "reference_date", "rules_version", "batch_window_start", "batch_window_end"):
        if summary.get(field) != h.get(field):
            raise LoadError("INPUT_LINEAGE", f"Summary and handover disagree on {field}")
    logs = summary.get("tables", [])
    if len(logs) != len(OUTPUT_TABLES) or {r.get("table_name") for r in logs} != set(OUTPUT_TABLES):
        raise LoadError("INPUT_COUNTS", "Summary must identify all 13 tables exactly once")
    expected = {r["table_name"]: r["records_output"] for r in logs}
    for field in ("transformed_datasets", "output_checksums", "content_checksums"):
        if set(h.get(field, {})) != set(OUTPUT_TABLES):
            raise LoadError("INPUT_COMPLETENESS", f"Expected all 13 entries in {field}")
    rows, counts, size = {}, {}, 0
    for table in OUTPUT_TABLES:
        table_log_path = root / "logs" / "spartment-transformation" / h["processing_period"] / f"{run}_{table}.json"
        shutil.copyfile(table_log_path, folder / f"{table}_log.json")
        log = read_json(folder / f"{table}_log.json")
        if (log.get("run_id") != run or log.get("table_name") != table
                or log.get("records_output") != expected[table]
                or log.get("output_checksum") != h["output_checksums"][table]):
            raise LoadError("INPUT_COUNTS", f"Inconsistent table metadata: {table}")
        destination = folder / f"{table}.csv"
        shutil.copyfile(safe_path(root, h["transformed_datasets"][table]), destination)
        if sha256(destination) != h["output_checksums"][table]:
            raise LoadError("CHECKSUM", f"CSV checksum mismatch: {table}")
        size += destination.stat().st_size
        parsed = []
        with destination.open(encoding="utf-8", newline="") as stream:
            reader = csv.reader(stream, strict=True)
            if tuple(next(reader, ())) != OUTPUT_COLUMNS[table]:
                raise LoadError("INPUT_SCHEMA", f"CSV header mismatch: {table}")
            for number, values in enumerate(reader, 2):
                if len(values) != len(OUTPUT_COLUMNS[table]):
                    raise LoadError("ROW_FORMAT", f"Malformed row: {table}, line {number}")
                try:
                    row = tuple(parse_value(table, c, x) for c, x in zip(OUTPUT_COLUMNS[table], values))
                    data = dict(zip(OUTPUT_COLUMNS[table], row))
                    if any(data[c] is None or data[c] == "" for c in REQUIRED_NOT_NULL[table]):
                        raise ValueError("required value missing")
                    if "transformation_run_id" in data and data["transformation_run_id"] != run:
                        raise ValueError("row lineage mismatch")
                    for column, domain in CODE_DOMAINS.get(table, {}).items():
                        if data[column] not in domain:
                            raise ValueError("code outside domain")
                    for column in NON_NEGATIVE.get(table, ()):
                        if data[column] is not None and data[column] < 0:
                            raise ValueError("negative value")
                    parsed.append(row)
                except (ValueError, InvalidOperation, OverflowError) as exc:
                    # Keys/values and database messages may contain personal data.
                    error = LoadError("ROW_VALUE", f"Invalid {table} line {number}; {type(exc).__name__}")
                    error.table_name = table
                    error.line_number = number
                    # Count only records actually read, not unverified future files.
                    error.received_counts = {**counts, table: number - 1}
                    raise error from exc
        if type(expected[table]) is not int or len(parsed) != expected[table]:
            raise LoadError("INPUT_COUNTS", f"CSV row count mismatch: {table}")
        rows[table], counts[table] = parsed, len(parsed)
    validate_relationships(rows)
    return Package(h, rows, counts, folder, size)


def validate_relationships(rows: dict[str, list[tuple]]) -> None:
    dictionaries = {t: [dict(zip(OUTPUT_COLUMNS[t], r)) for r in rows[t]] for t in OUTPUT_TABLES}
    keys = {}
    for table, records in dictionaries.items():
        for column in {primary_key(table), NATURAL_KEY[table]}:
            values = [r[column] for r in records]
            if len(set(values)) != len(values):
                raise LoadError("DUPLICATE_KEY", f"Duplicate {table}.{column}")
        keys[table] = {r[primary_key(table)] for r in records}
    for table, refs in FK_COLUMNS.items():
        for column, parent, nullable in refs:
            for row in dictionaries[table]:
                value = row[column]
                if (value is None and not nullable) or (value is not None and value not in keys[parent]):
                    raise LoadError("ORPHAN_KEY", f"Unmatched {table}.{column}")

"""Supabase connections, non-destructive role provisioning and capacity evidence."""
from __future__ import annotations

import json
import re
import secrets
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from .contract import OUTPUT_TABLES
from .models import LoadError


def is_supabase(host: str) -> bool:
    return host.endswith((".pooler.supabase.com", ".supabase.co"))


def validate_destination(dsn: str) -> dict:
    try:
        params = conninfo_to_dict(dsn)
    except Exception as exc:
        raise LoadError("CONFIG", "Invalid destination connection syntax; credentials suppressed") from exc
    host = params.get("host", "")
    if is_supabase(host):
        if params.get("port", "5432") != "5432":
            raise LoadError("CONFIG", "Use the Supabase direct or session connection on port 5432, not transaction pooling")
        if params.get("sslmode") not in {"require", "verify-ca", "verify-full"}:
            raise LoadError("CONFIG", "Supabase destination requires TLS")
        if params.get("dbname") != "postgres":
            raise LoadError("CONFIG", "Use the actual Supabase database name shown by Connect, normally postgres")
    elif host not in {"localhost", "127.0.0.1", "::1", ""} or params.get("dbname") != "spartment_warehouse":
        raise LoadError("CONFIG", "Destination must be the configured Supabase warehouse or local activity database")
    return params


def project_ref(params: dict) -> str:
    host = params.get("host", "")
    if host.endswith(".pooler.supabase.com"):
        parts = params.get("user", "").rsplit(".", 1)
        if len(parts) != 2 or not re.fullmatch("[a-z0-9]+", parts[1]):
            raise LoadError("CONFIG", "Session-pooler username must include the destination project reference")
        return parts[1]
    if host.startswith("db.") and host.endswith(".supabase.co"):
        return host.split(".")[1]
    return "local-test"


def capacity(conn, settings, csv_bytes: int, staged: int) -> dict:
    used = conn.execute("SELECT pg_database_size(current_database())").fetchone()[0]
    existing = sum(conn.execute("SELECT pg_total_relation_size(%s)", ("warehouse." + t,)).fetchone()[0] for t in OUTPUT_TABLES)
    # This is a configured conservative database budget, not a measurement of
    # the managed host's filesystem. Dashboard evidence covers physical headroom.
    reserve = settings.hosted_reserve_bytes + 3 * (staged + existing)
    quota_ok = used + reserve <= settings.database_budget_bytes
    local = shutil.disk_usage(settings.root)
    local_required = csv_bytes + 100 * 1024**2 + 2 * max(staged, existing)
    local_ok = local.free >= local_required
    physical = None
    reason = "No fresh project-bound Supabase disk-headroom evidence"
    if settings.capacity_evidence:
        try:
            data = json.loads(settings.capacity_evidence.read_text(encoding="utf-8"))
            timestamp = datetime.fromisoformat(data["observed_at"])
            reference = project_ref(conninfo_to_dict(settings.dsn))
            age = datetime.now(timezone.utc) - timestamp
            if (timestamp.tzinfo is not None and timedelta(0) <= age <= timedelta(hours=24)
                    and data.get("project_ref") == reference and data.get("source")
                    and type(data.get("available_disk_bytes")) is int and data["available_disk_bytes"] >= 0):
                physical = data["available_disk_bytes"]
                reason = "Fresh operator-recorded dashboard evidence"
        except (OSError, ValueError, KeyError, TypeError):
            pass
    return dict(mode="hosted", database_used_bytes=used, configured_database_budget_bytes=settings.database_budget_bytes,
                reserve_bytes=reserve, measured_staging_bytes=staged, existing_relation_bytes=existing,
                local_free_bytes=local.free, local_required_bytes=local_required,
                physical_disk_available_bytes=physical, physical_evidence=reason,
                sufficient=quota_ok and local_ok and (physical is None or physical >= reserve),
                pressure=(physical is None or (local.free-local_required)/local.total < settings.warning_free_ratio))


def provision_existing(owner_dsn: str, env_file: Path, role: str = "spartment_pipeline_loader") -> str:
    """Create a new restricted login; never reset the pre-existing loader role."""
    from .backend import ExistingWarehouse
    params = validate_destination(owner_dsn)
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,50}", role):
        raise LoadError("CONFIG", "Invalid runtime role name")
    with psycopg.connect(owner_dsn, autocommit=True, connect_timeout=10) as conn:
        ExistingWarehouse().validate(conn)
        with conn.transaction():
            if conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone():
                raise LoadError("ROLE_EXISTS", "Runtime role already exists; use its saved credentials, never reset it implicitly")
            password = secrets.token_urlsafe(32)
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))
            role_id = sql.Identifier(role)
            database = conn.execute("SELECT current_database()").fetchone()[0]
            conn.execute(sql.SQL("GRANT CONNECT,TEMP ON DATABASE {} TO {}").format(sql.Identifier(database), role_id))
            conn.execute(sql.SQL("GRANT USAGE ON SCHEMA warehouse,warehouse_meta TO {}").format(role_id))
            conn.execute(sql.SQL("GRANT SELECT,INSERT,DELETE ON ALL TABLES IN SCHEMA warehouse TO {}").format(role_id))
            conn.execute(sql.SQL("GRANT SELECT ON ALL TABLES IN SCHEMA warehouse_meta TO {}").format(role_id))
            conn.execute(sql.SQL("GRANT INSERT,UPDATE ON warehouse_meta.attempts TO {}").format(role_id))
            conn.execute(sql.SQL("GRANT INSERT ON warehouse_meta.batches TO {}").format(role_id))
            conn.execute(sql.SQL("GRANT INSERT,UPDATE ON warehouse_meta.current_batch TO {}").format(role_id))
    username = role + "." + project_ref(params) if params.get("host", "").endswith(".pooler.supabase.com") else role
    params.update(user=username, password=password)
    runtime = make_conninfo(**params)
    text = env_file.read_text(encoding="utf-8") if env_file.exists() else ""
    lines = [line for line in text.splitlines() if not line.startswith(("SPARTMENT_LOAD_DSN=", "SPARTMENT_LOAD_STORAGE_PATHS="))]
    lines.append("SPARTMENT_LOAD_DSN=" + runtime)
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return runtime

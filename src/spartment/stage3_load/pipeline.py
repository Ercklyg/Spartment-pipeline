"""Freeze, stage, reconcile and atomically publish a complete analytical snapshot."""
from __future__ import annotations

import csv
import json
import shutil
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable
from uuid import uuid4

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from . import __version__
from .backup import backup
from .contract import LINEAGE, LOCK_ID, OUTPUT_COLUMNS, OUTPUT_TABLES, SCHEMA_VERSION
from .models import LoadError, Settings, atomic_json
from .package import freeze, read_json
from .schema import check_schema
from .backend import selected_backend

PHT = timezone(timedelta(hours=8))
TRANSIENT = {"40P01", "40001", "55P03", "08000", "08003", "08006", "57P03"}


def now() -> str:
    return datetime.now(PHT).isoformat(timespec="seconds")


def new_run_id() -> str:
    return datetime.now(PHT).strftime("spartment-load-%Y-%m-%d-%H%M%S-") + uuid4().hex[:8]


def _table_detail(received: int) -> dict:
    return dict(records_expected=received, records_received=received, records_staged=0,
                records_inserted=0, records_updated=0, records_loaded=0, records_rejected=0,
                records_rolled_back=0, records_not_attempted=received, records_replaced=0,
                pre_load_result="NOT_RUN", post_load_result="NOT_RUN")


def load(settings: Settings, handover: Path, *, dry_run: bool = False,
         warning_review: dict | None = None, retry_of: str | None = None,
         attempt_number: int = 1, hook: Callable | None = None) -> dict:
    """The optional hook is an integration-test seam, never a CLI failure flag."""
    root = settings.root.resolve()
    run_id = new_run_id()
    started = time.monotonic()
    day = datetime.now(PHT).date().isoformat()
    log_path = root / "logs" / "spartment-load" / day / f"{run_id}.json"
    completion_path = root / "loaded" / "_handover" / f"{run_id}.json"
    result = dict(run_id=run_id, status="RUNNING", acceptance_decision=None, published=False,
                  load_started_at=now(), load_code_version=__version__, target_schema_version=SCHEMA_VERSION,
                  target_schema="warehouse", load_type="FULL", write_logic="REPLACE_SNAPSHOT",
                  checks=[], tables={}, retryable=False, attempt_number=attempt_number,
                  retry_of_run_id=retry_of, notification_status="PENDING", dry_run=dry_run)
    result["resource_settings"] = {key: getattr(settings, key) for key in (
        "budget_seconds", "warning_seconds", "min_free_bytes", "warning_free_ratio", "lock_seconds")}
    conn = None
    locked = False
    committed = False
    committing = False
    package = None
    backend = None

    def persist():
        result["load_duration_ms"] = int((time.monotonic() - started) * 1000)
        atomic_json(log_path, result)

    def check(check_id: str, passed: bool, evidence=None, *, warning=False):
        result["checks"].append(dict(check_id=check_id, severity="NON_CRITICAL" if warning else "CRITICAL",
                                     result="PASS" if passed else ("WARNING" if warning else "FAIL"),
                                     evidence=evidence))
        persist()
        if not passed and not warning:
            raise LoadError(check_id, f"Check failed: {check_id}")

    def budget():
        remaining = settings.budget_seconds - (time.monotonic() - started)
        if remaining <= 0:
            raise LoadError("DEADLINE", "Load deadline exceeded; publication cancelled")
        return remaining

    def execute(query, params=None):
        conn.execute("SELECT set_config('statement_timeout',%s,false)", (str(max(1, int(budget() * 1000))),))
        return conn.execute(query, params)

    def audit():
        if conn is None or conn.closed or backend is None:
            return
        backend.audit(conn, result)

    persist()
    try:
        initial = read_json(handover)
        result.update(input_handover=str(handover), transformation_run_id=initial.get("run_id"),
                      batch_id=initial.get("batch_id"), parent_run_id=initial.get("parent_run_id"))
        package = freeze(root, handover, root / "loaded" / "_work" / run_id)
        h = package.handover
        result.update(batch_id=h["batch_id"], transformation_run_id=h["run_id"], parent_run_id=h["parent_run_id"],
                      processing_period=h["processing_period"], reference_date=h["reference_date"],
                      batch_window_start=h["batch_window_start"], batch_window_end=h["batch_window_end"],
                      rules_version=h["rules_version"], output_checksums=h["output_checksums"],
                      content_checksums=h["content_checksums"],
                      tables={t: _table_detail(n) for t, n in package.counts.items()})
        check("PRE-04/05/06/07/13", True, "Complete approved package, lineage, checksums, headers and counts verified")
        conn = psycopg.connect(settings.dsn, autocommit=True, connect_timeout=10)
        result["target_database"] = conn.info.dbname
        execute("SET TIME ZONE 'Asia/Manila'")
        execute("SELECT set_config('lock_timeout', %s, false)", (str(settings.lock_seconds * 1000),))
        check("PRE-01", True)
        backend = selected_backend(conn, settings.target_profile)
        backend.validate(conn)
        backend.validate_values(package)
        result["target_profile"] = backend.profile
        result["target_schema_version"] = backend.schema_version
        check("PRE-03", True, "Live schema catalog matches initialized contract")
        check("PRE-02", _permissions(conn), "Runtime write and TEMP privileges")
        deadline = time.monotonic() + settings.lock_seconds
        while not execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,)).fetchone()[0]:
            if time.monotonic() >= deadline:
                raise LoadError("LOCK_TIMEOUT", "Another loader holds the publication lock", retryable=True)
            time.sleep(min(0.2, budget()))
        locked = True
        audit()
        duplicate = backend.duplicate(execute, h)
        if duplicate:
            check("PRE-09", True, "Committed batch identity and artifact comparison")
            result.update(status="SKIPPED_ALREADY_LOADED", acceptance_decision="APPROVED",
                          original_success_run_id=duplicate)
            for detail in result["tables"].values():
                detail["post_load_result"] = "NOT_APPLICABLE"
            return result
        check("PRE-09", True, "Batch not previously committed")
        prior = backend.current(execute)
        check("PRE-10", backend.fresh(prior, h), "Freshness and exclusive lock")
        result["previous_batch_id"] = prior[0] if prior else None
        with conn.transaction():
            for table in OUTPUT_TABLES:
                result.update(failed_table=table, failed_step="COPY_STAGING")
                result["tables"][table]["load_started_at"] = now()
                result["tables"][table]["_started_perf"] = time.monotonic()
                stage = sql.Identifier("stage_" + table)
                execute(sql.SQL("CREATE TEMP TABLE {} (LIKE warehouse.{} INCLUDING CONSTRAINTS INCLUDING INDEXES) ON COMMIT DROP").format(stage, sql.Identifier(table)))
                # LIKE includes lineage NOT NULL; staging receives only analytical columns.
                for c in backend.lineage:
                    execute(sql.SQL("ALTER TABLE {} DROP COLUMN {}").format(stage, sql.Identifier(c)))
                columns = sql.SQL(",").join(map(sql.Identifier, OUTPUT_COLUMNS[table]))
                budget()
                with conn.cursor().copy(sql.SQL("COPY {} ({}) FROM STDIN").format(stage, columns)) as copy:
                    for row in package.rows[table]:
                        budget()
                        copy.write_row(row)
                result["tables"][table]["records_staged"] = package.counts[table]
                result["tables"][table]["pre_load_result"] = "PASS"
                result["tables"][table]["records_not_attempted"] = 0
            check("PRE-11", True, "All rows staged with lossless types; keys and references checked in input")
            relation_bytes = sum(execute("SELECT pg_total_relation_size(%s)", ("pg_temp.stage_" + t,)).fetchone()[0]
                                 for t in OUTPUT_TABLES)
            storage = _storage(conn, settings, package.bytes_received, relation_bytes)
            check("PRE-08", storage["sufficient"], storage)
            check("PRE-12", not storage["pressure"], storage, warning=True)
            result["measured_staging_bytes"] = relation_bytes
            warnings = [c["check_id"] for c in result["checks"] if c["result"] == "WARNING"]
            if hook:
                hook("staged", conn)
            if dry_run:
                raise _DryRun()
            _review(warnings, warning_review, h, result)
            for table in OUTPUT_TABLES:
                execute(sql.SQL("LOCK TABLE warehouse.{} IN SHARE ROW EXCLUSIVE MODE").format(sql.Identifier(table)))
            registry = "current_batch" if backend.profile == "supabase_existing" else "current_snapshot"
            execute(sql.SQL("LOCK TABLE {}.{} IN SHARE ROW EXCLUSIVE MODE").format(sql.Identifier(backend.metadata_schema), sql.Identifier(registry)))
            check("PRE-10-RECHECK", backend.current(execute) == prior and backend.duplicate(execute, h) is None,
                  "Current snapshot unchanged while staging; writer locks now held")
            if prior:
                result["backup_path"] = str(backup(settings.owner_dsn or settings.dsn, root, run_id,
                    settings.pg_bin, max(1, min(300, int(budget()))), metadata_schema=backend.metadata_schema))
            for table in reversed(OUTPUT_TABLES):
                result.update(failed_table=table, failed_step="DELETE_PREVIOUS")
                result["tables"][table]["records_replaced"] = execute(sql.SQL("DELETE FROM warehouse.{}").format(sql.Identifier(table))).rowcount
            if hook:
                hook("deleted", conn)
            for table in OUTPUT_TABLES:
                result.update(failed_table=table, failed_step="INSERT_AND_RECONCILE")
                columns = sql.SQL(",").join(map(sql.Identifier, OUTPUT_COLUMNS[table]))
                names, values = backend.insert_lineage(run_id, h, result["load_started_at"])
                lineage = sql.SQL(",").join(map(sql.Identifier, names))
                placeholders = sql.SQL(",").join(sql.Placeholder() for _ in values)
                execute(sql.SQL("INSERT INTO warehouse.{} ({},{}) SELECT {},{} FROM {}")
                        .format(sql.Identifier(table), columns, lineage, columns, placeholders,
                                sql.Identifier("stage_" + table)), values)
                if hook:
                    hook("inserted:" + table, conn)
                _reconcile(execute, table, h["batch_id"], package.counts[table], result["tables"][table], check)
            check("POST-03/04/05/08", True, "Enabled PK, UQ, FK, CHECK and NOT NULL constraints passed for all tables")
            elapsed = time.monotonic() - started
            check("POST-10", elapsed <= settings.warning_seconds, {"duration_seconds": round(elapsed, 3)}, warning=True)
            warnings = [c["check_id"] for c in result["checks"] if c["result"] == "WARNING"]
            _review(warnings, warning_review, h, result)
            budget()
            for detail in result["tables"].values():
                detail.update(records_inserted=detail["records_received"], records_loaded=detail["records_received"],
                              records_not_attempted=0, post_load_result="PASS")
            result.update(status="SUCCESS", acceptance_decision="APPROVED", published=True, published_at=now())
            result.update(failed_table=None, failed_step=None)
            backend.publish(execute, h, result, package.counts)
            check("POST-09", True, "Replacement rows and registry share the batch identity")
            audit()
            budget()
            committing = True
        committed = True
        check("POST-11", backend.committed(execute, h["batch_id"]) == run_id, "Commit registry confirmed")
    except _DryRun:
        result.update(status="DRY_RUN", acceptance_decision=None, published=False)
    except _Held:
        result.update(status="HELD", acceptance_decision="MANUAL REVIEW", published=False)
    except Exception as exc:
        code = exc.code if isinstance(exc, LoadError) else getattr(exc, "sqlstate", None) or type(exc).__name__
        connection_transient = False
        if isinstance(exc, psycopg.OperationalError) and not getattr(exc, "sqlstate", None):
            private_message = str(exc).lower()
            if "password authentication failed" in private_message or "no password supplied" in private_message:
                code = "AUTHENTICATION"
            elif any(token in private_message for token in ("connection refused", "timeout", "connection reset", "server closed the connection")):
                code, connection_transient = "CONNECTION_TRANSIENT", True
        # Never include raw SQL exceptions, row values, DSNs, or traceback in artifacts.
        result.update(status="FAILED", acceptance_decision="REJECTED", published=False,
                      error_code=code, error_message=str(exc) if isinstance(exc, LoadError) else "Load failed; inspect error code and table-step evidence",
                      sqlstate=getattr(exc, "sqlstate", None),
                      retryable=(exc.retryable if isinstance(exc, LoadError) else code in TRANSIENT or connection_transient))
        phase = "POST-" if result.get("failed_step") in {"DELETE_PREVIOUS", "INSERT_AND_RECONCILE"} or committing else "PRE-"
        result["checks"].append(dict(check_id=phase + "ERROR:" + code, severity="CRITICAL", result="FAIL"))
        if committing:
            # A connection/acknowledgement failure may occur after the server committed.
            try:
                with psycopg.connect(settings.dsn, autocommit=True, connect_timeout=10) as probe:
                    row = backend.committed(probe.execute, result["batch_id"])
                    if row == run_id:
                        committed = True
                        result.update(status="SUCCESS", acceptance_decision="APPROVED", published=True, retryable=False,
                                      commit_recovered=True)
                    elif row is None and (conn is None or conn.closed):
                        result["retryable"] = True
                    else:
                        result.update(status="COMMIT_UNKNOWN", acceptance_decision=None, retryable=False)
            except Exception:
                result.update(status="COMMIT_UNKNOWN", acceptance_decision=None, retryable=False)
        if code == "ROW_VALUE":
            result["tables"] = {t: _table_detail(n) for t, n in exc.received_counts.items()}
            detail = result["tables"][exc.table_name]
            detail.update(records_rejected=1, records_not_attempted=detail["records_received"] - 1,
                          pre_load_result="FAIL")
            path = root / "quarantine" / "load" / day / run_id / f"{exc.table_name}_rejected.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="", encoding="utf-8") as stream:
                writer = csv.writer(stream)
                writer.writerow(["run_id", "table_name", "line_number", "check_id", "reason"])
                writer.writerow([run_id, exc.table_name, exc.line_number, code, result["error_message"]])
    finally:
        if not committed:
            for detail in result["tables"].values():
                detail.update(records_inserted=0, records_loaded=0, records_updated=0,
                              records_rolled_back=detail["records_staged"], records_replaced=0)
        result["load_ended_at"] = now()
        for detail in result["tables"].values():
            perf = detail.pop("_started_perf", None)
            detail["load_ended_at"] = now() if perf is not None else None
            detail["load_duration_ms"] = int((time.monotonic() - perf) * 1000) if perf is not None else 0
        result["pre_load_result"] = _phase(result, "PRE-")
        result["post_load_result"] = _phase(result, "POST-")
        result["notification_status"] = "DISPLAYED"
        try:
            audit()
        except Exception:
            result["audit_database_pending"] = True
        try:
            persist()
            if result["status"] != "RUNNING":
                atomic_json(completion_path, result)
            for table, detail in result["tables"].items():
                atomic_json(log_path.with_name(f"{run_id}_{table}.json"), {"run_id": run_id, "table_name": table, **detail})
        finally:
            if conn and not conn.closed:
                try:
                    if locked:
                        conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))
                finally:
                    conn.close()
        if (package and result["status"] in {"SUCCESS", "SKIPPED_ALREADY_LOADED", "DRY_RUN"}
                and package.folder.resolve().is_relative_to((root / "loaded" / "_work").resolve())):
            shutil.rmtree(package.folder)
    return result


class _DryRun(Exception):
    pass


class _Held(Exception):
    pass


def _phase(result: dict, prefix: str) -> str:
    checks = [c["result"] for c in result["checks"] if c["check_id"].startswith(prefix)]
    return "FAIL" if "FAIL" in checks else "WARNING" if "WARNING" in checks else "PASS" if checks else "NOT_RUN"


def _review(warnings: list, review: dict | None, h: dict, result: dict) -> None:
    if not warnings:
        return
    if (not review or review.get("batch_id") != h["batch_id"]
            or review.get("output_checksums") != h["output_checksums"]
            or not set(warnings) <= set(review.get("waived_checks", []))
            or not all(review.get(c) for c in ("reviewed_by", "reviewed_at", "review_reason"))):
        raise _Held()
    result["warning_review"] = review


def _permissions(conn) -> bool:
    database = conn.execute("SELECT current_database(),current_setting('transaction_read_only')").fetchone()
    if database[1] != "off" or not conn.execute("SELECT has_database_privilege(current_user,%s,'TEMP')", (database[0],)).fetchone()[0]:
        return False
    for table in OUTPUT_TABLES:
        for privilege in ("SELECT", "INSERT", "DELETE"):
            if not conn.execute("SELECT has_table_privilege(current_user,%s,%s)", ("warehouse." + table, privilege)).fetchone()[0]:
                return False
    return True


def _storage(conn, settings: Settings, csv_bytes: int, staged: int) -> dict:
    host = conn.info.host or ""
    from .hosted import is_supabase
    if settings.capacity_mode == "hosted" or (settings.capacity_mode == "auto" and is_supabase(host)):
        from .hosted import capacity
        return capacity(conn, settings, csv_bytes, staged)
    paths = list(settings.storage_paths)
    if not paths:
        try:
            paths = [Path(conn.execute("SHOW data_directory").fetchone()[0])]
        except psycopg.Error as exc:
            raise LoadError("STORAGE_PATH", "Configure SPARTMENT_LOAD_STORAGE_PATHS for the local PostgreSQL data/WAL/tablespace volumes") from exc
    extra = conn.execute("SELECT pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname NOT IN ('pg_default','pg_global')").fetchall()
    paths += [Path(r[0]) for r in extra if r[0]]
    # WAL may be relocated via a symlink; resolve it to include that volume.
    for p in list(paths):
        if (p / "pg_wal").exists():
            paths.append((p / "pg_wal").resolve())
    existing = sum(conn.execute("SELECT pg_total_relation_size(%s)", ("warehouse." + t,)).fetchone()[0] for t in OUTPUT_TABLES)
    requirements = [(p, settings.min_free_bytes + (staged + existing) * 3) for p in paths]
    requirements += [(settings.root, csv_bytes + 100 * 1024**2 + max(staged, existing) * 2)]
    volumes = {}
    for p, needed in requirements:
        resolved = p.resolve()
        while not resolved.exists() and resolved != resolved.parent:
            resolved = resolved.parent
        usage = shutil.disk_usage(resolved)
        # Windows drive; on Unix use device ID, covering mountpoints correctly.
        key = str(resolved.drive).lower() if resolved.drive else str(resolved.stat().st_dev)
        record = volumes.setdefault(key, {"free_bytes": usage.free, "total_bytes": usage.total, "required_bytes": 0})
        record["required_bytes"] += needed
    return dict(volumes=volumes, measured_staging_bytes=staged, existing_relation_bytes=existing,
                sufficient=all(v["free_bytes"] >= v["required_bytes"] for v in volumes.values()),
                pressure=any((v["free_bytes"] - v["required_bytes"]) / v["total_bytes"] < settings.warning_free_ratio for v in volumes.values()))


def _reconcile(execute, table: str, batch_id: str, expected: int, detail: dict, check):
    target, stage = sql.Identifier("warehouse", table), sql.Identifier("stage_" + table)
    count, batch_count = execute(sql.SQL("SELECT count(*),count(*) FILTER(WHERE load_batch_id=%s) FROM {}").format(target), (batch_id,)).fetchone()
    check("POST-01/02:" + table, count == batch_count == expected, {"expected": expected, "target": count})
    cols = sql.SQL(",").join(map(sql.Identifier, OUTPUT_COLUMNS[table]))
    differences = execute(sql.SQL("SELECT EXISTS((SELECT {} FROM {} EXCEPT ALL SELECT {} FROM {}) UNION ALL (SELECT {} FROM {} EXCEPT ALL SELECT {} FROM {}))")
                          .format(cols, target, cols, stage, cols, stage, cols, target)).fetchone()[0]
    check("POST-07:" + table, not differences, "Typed incoming values equal target in both directions")
    from .contract import OUTPUT_TYPES
    totals = {}
    for c in OUTPUT_COLUMNS[table]:
        if OUTPUT_TYPES[table][c] == "money":
            incoming = execute(sql.SQL("SELECT coalesce(sum({}),0) FROM {}").format(sql.Identifier(c), stage)).fetchone()[0]
            outgoing = execute(sql.SQL("SELECT coalesce(sum({}),0) FROM {}").format(sql.Identifier(c), target)).fetchone()[0]
            totals[c] = {"source": str(incoming), "target": str(outgoing)}
            check("POST-06:" + table + "." + c, incoming == outgoing, totals[c])
    detail["money_totals"] = totals

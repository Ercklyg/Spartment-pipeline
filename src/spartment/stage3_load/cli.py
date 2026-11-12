"""Owner setup and normal loader commands; credentials never enter argv."""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime
from pathlib import Path

from spartment.common.config import REPO_ROOT, load_env, load_toml

from .models import LoadError, Settings


def environment_settings(root: Path = REPO_ROOT) -> Settings:
    load_env()
    dsn = os.environ.get("SPARTMENT_LOAD_DSN", "")
    if not dsn:
        raise LoadError("CONFIG", "Set SPARTMENT_LOAD_DSN in .env after owner setup")
    from .hosted import validate_destination
    params = validate_destination(dsn)
    thresholds = load_toml(root / "config" / "load.toml").get("load", {})
    allowed = {"budget_seconds", "warning_seconds", "lock_seconds", "min_free_bytes", "warning_free_ratio",
               "target_profile", "capacity_mode", "database_budget_bytes", "hosted_reserve_bytes"}
    if set(thresholds) - allowed:
        raise LoadError("CONFIG", "Unknown setting in config/load.toml")
    defaults = Settings(dsn=dsn)
    for key, value in thresholds.items():
        if key in {"target_profile", "capacity_mode"}:
            choices = {"auto", "local", "supabase_existing"} if key == "target_profile" else {"auto", "local", "hosted"}
            if value not in choices:
                raise LoadError("CONFIG", f"Invalid {key}")
            continue
        if type(value) not in {int, float} or value < 0:
            raise LoadError("CONFIG", f"Invalid {key} in config/load.toml")
    if not 0 <= thresholds.get("warning_free_ratio", defaults.warning_free_ratio) <= 1:
        raise LoadError("CONFIG", "warning_free_ratio must be between zero and one")
    if thresholds.get("budget_seconds", defaults.budget_seconds) <= 0:
        raise LoadError("CONFIG", "budget_seconds must be positive")
    return Settings(dsn=dsn, root=root, owner_dsn=os.environ.get("SPARTMENT_LOAD_OWNER_DSN") or None,
                    pg_bin=Path(os.environ["SPARTMENT_PG_BIN"]) if os.environ.get("SPARTMENT_PG_BIN") else None,
                    capacity_evidence=Path(os.environ["SPARTMENT_LOAD_CAPACITY_EVIDENCE"]) if os.environ.get("SPARTMENT_LOAD_CAPACITY_EVIDENCE") else None,
                    storage_paths=tuple(Path(p) for p in os.environ.get("SPARTMENT_LOAD_STORAGE_PATHS", "").split(";") if p),
                    **thresholds)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Load an approved Spartment analytical snapshot into local PostgreSQL")
    commands = parser.add_subparsers(dest="command", required=True)
    setup = commands.add_parser("setup", help="Owner-only creation of the warehouse schema")
    setup.add_argument("--provision-role", action="store_true", help="Create restricted login and save its generated credential to .env")
    commands.add_parser("schema-sql", help="Print schema DDL for review without connecting")
    inspect = commands.add_parser("inspect", help="Read-only contract, counts and capacity inspection; no schema or data writes")
    inspect.add_argument("--owner", action="store_true", help="Use owner DSN before runtime provisioning")
    provision = commands.add_parser("provision-existing", help="Add a new restricted runtime login for the reviewed warehouse without changing existing tables or roles")
    provision.add_argument("--role", default="spartment_pipeline_loader", help="New role name; existing roles are never reset")
    run = commands.add_parser("run", help="Validate and publish one exact Stage 2 handover")
    run.add_argument("--handover", type=Path, required=True)
    run.add_argument("--dry-run", action="store_true", help="Stage and validate, roll back without publication")
    run.add_argument("--review-file", type=Path, help="Recorded waiver of non-critical load checks for this exact batch")
    run.add_argument("--retry-transient", action="store_true", help="At most three retries: 5, 10, 20 minute delays")
    run.add_argument("--deadline", help="ISO timestamp with timezone; do not start or finish after this deadline")
    recover = commands.add_parser("reconcile", help="Resolve an interrupted/unknown attempt from committed database evidence")
    recover.add_argument("--run-id", required=True)
    args = parser.parse_args(argv)
    try:
        load_env()
        if args.command == "schema-sql":
            from .schema import statements
            print(";\n\n".join(statements()) + ";")
            return 0
        if args.command == "setup":
            from .schema import initialize, provision
            owner = os.environ.get("SPARTMENT_LOAD_OWNER_DSN", "")
            from .hosted import validate_destination, is_supabase
            params = validate_destination(owner)
            if is_supabase(params.get("host", "")):
                raise LoadError("EXISTING_TARGET", "Use inspect and provision-existing for Supabase; setup must not recreate its warehouse")
            created = initialize(owner)
            if args.provision_role:
                provision(owner, REPO_ROOT / ".env")
            print(json.dumps({"schema": "CREATED" if created else "VERIFIED", "runtime_role_provisioned": args.provision_role}))
            return 0
        if args.command == "provision-existing":
            from .hosted import provision_existing
            provision_existing(os.environ.get("SPARTMENT_LOAD_OWNER_DSN", ""), REPO_ROOT / ".env", role=args.role)
            print(json.dumps({"runtime_role_provisioned": True, "existing_warehouse_preserved": True}))
            return 0
        if args.command == "inspect":
            import psycopg
            from psycopg import sql
            from .backend import selected_backend
            from .contract import OUTPUT_TABLES
            from .hosted import validate_destination
            dsn = os.environ.get("SPARTMENT_LOAD_OWNER_DSN" if args.owner else "SPARTMENT_LOAD_DSN", "")
            validate_destination(dsn)
            with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
                with conn.transaction():
                    conn.execute("SET TRANSACTION READ ONLY")
                    backend = selected_backend(conn)
                    backend.validate(conn)
                    counts = {t: conn.execute(sql.SQL("SELECT count(*) FROM warehouse.{}").format(sql.Identifier(t))).fetchone()[0] for t in OUTPUT_TABLES}
                    print(json.dumps({"connection": "PASS", "schema": "PASS", "profile": backend.profile,
                                      "database": conn.info.dbname, "counts": counts,
                                      "database_size_bytes": conn.execute("SELECT pg_database_size(current_database())").fetchone()[0],
                                      "warehouse_writes": 0}))
            return 0
        settings = environment_settings()
        if args.command == "reconcile":
            from .recovery import reconcile
            print(json.dumps(reconcile(settings, args.run_id), default=str))
            return 0
        from .pipeline import load
        review = json.loads(args.review_file.read_text(encoding="utf-8")) if args.review_file else None
        deadline = datetime.fromisoformat(args.deadline) if args.deadline else None
        if deadline and deadline.tzinfo is None:
            raise LoadError("CONFIG", "Deadline must include timezone")
        previous = None
        for attempt in range(1, 5 if args.retry_transient else 2):
            if deadline:
                remaining = (deadline - datetime.now(deadline.tzinfo)).total_seconds()
                if remaining <= 0:
                    raise LoadError("DEADLINE", "Execution window closed; start a manual run in a new window")
                settings.budget_seconds = min(settings.budget_seconds, int(remaining))
            result = load(settings, args.handover, dry_run=args.dry_run, warning_review=review,
                          retry_of=previous, attempt_number=attempt)
            print(json.dumps({key: result.get(key) for key in ("run_id", "status", "acceptance_decision", "published", "error_code", "retryable")}))
            status = result["status"]
            if status in {"SUCCESS", "SKIPPED_ALREADY_LOADED", "DRY_RUN"}:
                return 0
            if status == "HELD":
                return 5
            if not result.get("retryable") or attempt == 4 or not args.retry_transient:
                return 6 if status == "COMMIT_UNKNOWN" else 1
            wait = (300, 600, 1200)[attempt - 1]
            if deadline and (deadline - datetime.now(deadline.tzinfo)).total_seconds() <= wait:
                return 1
            # Pollable waits support graceful interruption and avoid a single long sleep.
            for _ in range(wait):
                time.sleep(1)
            previous = result["run_id"]
    except LoadError as exc:
        print(json.dumps({"error_code": exc.code, "message": str(exc)}))
        return 2
    except Exception as exc:
        print(json.dumps({"error_code": getattr(exc, "sqlstate", None) or type(exc).__name__,
                          "message": "Command failed; verify local configuration. Credentials and database error details are suppressed."}))
        return 1
    return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Local PostgreSQL custom-format backups without credentials in argv or logs."""
from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from psycopg.conninfo import conninfo_to_dict

from .models import LoadError, atomic_json
from .package import sha256


def pg_environment(dsn: str) -> dict[str, str]:
    env = dict(os.environ)
    for key, value in conninfo_to_dict(dsn).items():
        if key in {"host", "port", "dbname", "user", "password", "sslmode"}:
            env["PG" + ("DATABASE" if key == "dbname" else key.upper())] = value
    env["PGCONNECT_TIMEOUT"] = "10"
    return env


def pg_tool(bin_dir: Path | None, tool: str) -> str:
    import shutil
    path = str(bin_dir / tool) if bin_dir else shutil.which(tool)
    if not path:
        raise LoadError("BACKUP_TOOL", f"PostgreSQL utility not configured: {tool}")
    return path


def backup(dsn: str, root: Path, run_id: str, pg_bin: Path | None, timeout: int = 300, *, metadata_schema: str = "load_control") -> Path:
    folder = root / "backups" / "spartment-warehouse"
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / f"{run_id}.dump"
    if path.exists():
        raise LoadError("BACKUP_EXISTS", "Backup path already exists")
    command = [pg_tool(pg_bin, "pg_dump"), "--format=custom", "--no-password",
               "--schema=warehouse", "--schema=" + metadata_schema, "--file", str(path)]
    try:
        subprocess.run(command, env=pg_environment(dsn), check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE, timeout=timeout)
        subprocess.run([pg_tool(pg_bin, "pg_restore"), "--list", str(path)], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=timeout)
    except (OSError, subprocess.SubprocessError) as exc:
        raise LoadError("BACKUP_FAILED", "Pre-refresh backup or archive readability check failed") from exc
    atomic_json(path.with_suffix(".json"), {
        "run_id": run_id, "path": str(path), "sha256": sha256(path),
        "created_at": datetime.now(timezone.utc).isoformat(), "readability": "PASS",
        "restore_tested": False,
    })
    return path


def restore(dsn: str, path: Path, pg_bin: Path | None) -> None:
    """Restore only into a separate, empty verification database."""
    import psycopg
    from .package import read_json
    manifest = read_json(path.with_suffix(".json"))
    if sha256(path) != manifest.get("sha256"):
        raise LoadError("BACKUP_CHECKSUM", "Backup checksum differs from manifest")
    with psycopg.connect(dsn, autocommit=True) as conn:
        if conn.execute("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.relkind='r' AND n.nspname NOT IN ('pg_catalog','information_schema')").fetchone()[0]:
            raise LoadError("RESTORE_NOT_EMPTY", "Restore target must be a separate empty database")
    try:
        subprocess.run([pg_tool(pg_bin, "pg_restore"), "--no-password", "--no-owner", "--no-acl",
                        "--exit-on-error", "--single-transaction", "--dbname",
                        conninfo_to_dict(dsn)["dbname"], str(path)],
                       env=pg_environment(dsn), check=True, stdout=subprocess.DEVNULL,
                       stderr=subprocess.PIPE, timeout=300)
    except (OSError, subprocess.SubprocessError) as exc:
        raise LoadError("RESTORE_FAILED", "Restore verification failed") from exc

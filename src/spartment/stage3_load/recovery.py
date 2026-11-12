"""Recover local completion evidence without replaying an uncertain batch."""
from __future__ import annotations

import re

import psycopg
from psycopg.types.json import Jsonb

from .contract import LOCK_ID
from .models import LoadError, Settings, atomic_json
from .package import read_json
from .pipeline import now
from .backend import selected_backend


def reconcile(settings: Settings, run_id: str) -> dict:
    if not re.fullmatch(r"spartment-load-[\w-]+", run_id):
        raise LoadError("INPUT_ID", "Invalid load run ID")
    matches = list((settings.root / "logs" / "spartment-load").glob(f"*/{run_id}.json"))
    with psycopg.connect(settings.dsn, autocommit=True) as conn:
        if not conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_ID,)).fetchone()[0]:
            raise LoadError("LOCK_TIMEOUT", "A load is still active; reconcile later")
        try:
            backend = selected_backend(conn, settings.target_profile)
            existing = backend.profile == "supabase_existing"
            row = conn.execute("SELECT report FROM warehouse_meta.attempts WHERE run_id=%s" if existing else
                               "SELECT detail FROM load_control.load_attempt WHERE run_id=%s", (run_id,)).fetchone()
            data = row[0] if row else read_json(matches[0]) if len(matches) == 1 else None
            if not data:
                raise LoadError("RECOVERY_RECORD", "No unique attempt record was found")
            committed = conn.execute("SELECT batch_id,report FROM warehouse_meta.batches WHERE run_id=%s" if existing else
                                     "SELECT batch_id,counts FROM load_control.loaded_batch WHERE run_id=%s", (run_id,)).fetchone()
            if committed:
                data.update(status="SUCCESS", acceptance_decision="APPROVED", published=True, commit_recovered=True)
                counts = ({t: d["records_loaded"] for t, d in committed[1]["tables"].items()} if existing else committed[1])
                for table, count in counts.items():
                    data["tables"][table].update(records_inserted=count, records_loaded=count,
                                                records_rolled_back=0, records_not_attempted=0)
            elif data["status"] in {"RUNNING", "COMMIT_UNKNOWN"}:
                data.update(status="INTERRUPTED", acceptance_decision="REJECTED", published=False,
                            retryable=True, error_code="INTERRUPTED_NO_COMMIT")
            data.update(load_ended_at=now(), notification_status="DISPLAYED", reconciled_at=now())
            backend.audit(conn, data)
            if matches:
                atomic_json(matches[0], data)
            atomic_json(settings.root / "loaded" / "_handover" / f"{run_id}.json", data)
            return data
        finally:
            conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))

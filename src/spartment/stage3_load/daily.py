"""Windows-friendly sequential daily batch; a scheduler starts this process."""
from __future__ import annotations

import argparse
import json
from datetime import datetime, timedelta

from spartment.common.config import REPO_ROOT, load_env
from .models import atomic_json
from .pipeline import PHT, new_run_id


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run extraction, transformation and load in dependency order")
    parser.add_argument("--check-only", action="store_true", help="Verify configuration without contacting source or publishing")
    args = parser.parse_args(argv)
    import os
    load_env()
    required = ("SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY", "SPARTMENT_LOAD_DSN")
    missing = [k for k in required if not os.environ.get(k) or "your-project-ref" in os.environ[k]]
    if missing:
        print(json.dumps({"status": "CONFIG_REQUIRED", "missing": missing}))
        return 2
    if args.check_only:
        print(json.dumps({"status": "CONFIG_PRESENT", "execution": "NOT_RUN"}))
        return 0
    from spartment.stage1_extract.cli import main as extract
    from spartment.stage2_transform.cli import main as transform
    from .cli import main as load
    from .pipeline import now
    run_id = new_run_id().replace("spartment-load-", "spartment-daily-")
    date = datetime.now(PHT).date().isoformat()
    evidence = {"run_id": run_id, "started_at": now(), "steps": [], "status": "RUNNING"}
    path = REPO_ROOT / "logs" / "spartment-daily" / date / f"{run_id}.json"
    atomic_json(path, evidence)
    try:
        # Exit 0 is required: no implicit release of an upstream warning batch.
        code = extract([])
        evidence["steps"].append({"stage": "extract", "exit_code": code})
        if code != 0:
            raise RuntimeError("Extraction did not approve the batch")
        handovers = []
        for p in (REPO_ROOT / "rawdata" / "_handover").glob("*.json"):
            h = json.loads(p.read_text(encoding="utf-8"))
            if h.get("processing_period") == date and h.get("acceptance_decision", {}).get("decision") == "APPROVED":
                handovers.append(p)
        parent = max(handovers, key=lambda p: p.stat().st_mtime)
        code = transform(["--parent-run-id", parent.stem, "--reference-date", date])
        evidence["steps"].append({"stage": "transform", "exit_code": code})
        if code != 0:
            raise RuntimeError("Transformation did not approve the batch")
        candidates = []
        for p in (REPO_ROOT / "transformed" / "_handover").glob("*.json"):
            h = json.loads(p.read_text(encoding="utf-8"))
            if h.get("parent_run_id") == parent.stem:
                candidates.append(p)
        transformed = max(candidates, key=lambda p: p.stat().st_mtime)
        deadline = datetime.now(PHT) + timedelta(minutes=30)
        code = load(["run", "--handover", str(transformed), "--retry-transient", "--deadline", deadline.isoformat()])
        evidence["steps"].append({"stage": "load", "exit_code": code})
        evidence["status"] = "SUCCESS" if code == 0 else "FAILED"
        return code
    except Exception as exc:
        evidence["status"] = "FAILED"
        evidence["error_type"] = type(exc).__name__
        return 1
    finally:
        evidence["ended_at"] = now()
        atomic_json(path, evidence)
        print(json.dumps(evidence))


if __name__ == "__main__":
    raise SystemExit(main())

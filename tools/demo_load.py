"""Generate synthetic Stage 2 output and optionally demonstrate a local load.

Uses the repository's synthetic test fixture, not operational tenant data.
Publication is refused when the target already contains a published snapshot.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "tests"))

from test_stage2 import base_rows, write_batch, settings as transform_settings, PARENT
from spartment.stage2_transform.pipeline import transform
from spartment.stage3_load.cli import environment_settings
from spartment.stage3_load.models import atomic_json
from spartment.stage3_load.pipeline import load


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--publish-demo", action="store_true", help="Publish synthetic data into an empty initialized activity target")
    parser.add_argument("--review-file", type=Path, help="Recorded waiver for non-critical warnings in this exact synthetic batch")
    args = parser.parse_args()
    folder = ROOT / "loaded" / "demo"
    folder.mkdir(parents=True, exist_ok=True)
    write_batch(folder, base_rows())
    transformation = transform(transform_settings(folder), "spartment-transform-2026-10-18-demo", PARENT)
    print(json.dumps({"data_kind": "SYNTHETIC_ACTIVITY_DEMO", "transformation_decision": transformation.decision,
                      "handover": str(transformation.handover_path),
                      "counts": {t: len(rows) for t, rows in transformation.outputs.items()}}))
    if not args.publish_demo:
        return 0
    import psycopg
    from spartment.stage3_load.backend import selected_backend
    settings = environment_settings(folder)
    with psycopg.connect(settings.dsn) as conn:
        if selected_backend(conn, settings.target_profile).current(conn.execute):
            raise RuntimeError("Target is not empty; use the normal loader on a reviewed handover instead of overwriting with demo data")
    dry = load(settings, transformation.handover_path, dry_run=True)
    if dry["status"] != "DRY_RUN":
        print(json.dumps({"dry_run_status": dry["status"], "error_code": dry.get("error_code")}))
        return 1
    review = json.loads(args.review_file.read_text(encoding="utf-8")) if args.review_file else None
    result = load(settings, transformation.handover_path, warning_review=review)
    duplicate = load(settings, transformation.handover_path) if result["status"] == "SUCCESS" else None
    evidence = {"data_kind": "SYNTHETIC_ACTIVITY_DEMO", "dry_run": dry,
                "publication": result, "duplicate": duplicate}
    atomic_json(folder / "demonstration.json", evidence)
    print(json.dumps({"publication": result["status"], "duplicate": duplicate["status"] if duplicate else None,
                      "evidence": str(folder / "demonstration.json")}))
    return 0 if result["status"] == "SUCCESS" and duplicate and duplicate["status"] == "SKIPPED_ALREADY_LOADED" else 1


if __name__ == "__main__":
    raise SystemExit(main())

from __future__ import annotations

import gzip
import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from spartment.stage2_transform.retention import apply_retention


def _file(path: Path, age_days: int, now: datetime, content: str = "{}\n") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    timestamp = (now - timedelta(days=age_days)).timestamp()
    os.utime(path, (timestamp, timestamp))
    return path


class RetentionTests(unittest.TestCase):
    def test_apply_archives_logs_and_requires_quarantine_opt_in(self) -> None:
        now = datetime(2026, 10, 18, 2, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            log_root = base / "logs" / "spartment-transformation"
            quarantine_root = base / "quarantine" / "transformation"

            old_table = _file(
                log_root / "2025-10-01" / "run_dim_tenant.json", 100, now
            )
            recent_table = _file(
                log_root / "2026-10-08" / "run_fact_billing.json", 10, now
            )
            old_summary_inside_audit = _file(
                log_root / "2025-10-01" / "run.json", 100, now
            )
            old_summary = _file(log_root / "2025-09-01" / "old.json", 400, now)
            old_quarantine_inside_audit = _file(
                quarantine_root / "2025-10-01" / "dim_room_rejected.csv", 100, now
            )
            old_quarantine = _file(
                quarantine_root / "2025-09-01" / "fact_payment_rejected.csv", 400, now
            )

            dry_run = apply_retention(
                log_root,
                quarantine_root,
                operational_days=90,
                audit_days=365,
                apply=False,
                now=now,
                timezone=timezone.utc,
            )
            self.assertEqual(
                {candidate.path for candidate in dry_run.candidates},
                {old_table, old_summary},
            )
            self.assertTrue(old_table.exists())
            self.assertTrue(old_summary.exists())
            self.assertTrue(old_quarantine.exists())

            applied = apply_retention(
                log_root,
                quarantine_root,
                operational_days=90,
                audit_days=365,
                apply=True,
                now=now,
                timezone=timezone.utc,
            )
            self.assertEqual(len(applied.archived), 2)
            self.assertEqual(len(applied.deleted), 2)
            self.assertFalse(old_table.exists())
            self.assertFalse(old_summary.exists())
            self.assertTrue(old_quarantine.exists())
            self.assertTrue(recent_table.exists())
            self.assertTrue(old_summary_inside_audit.exists())
            self.assertTrue(old_quarantine_inside_audit.exists())

            archived_paths = {Path(path) for path in applied.archived}
            self.assertIn(
                log_root / "_archive" / "2025-10-01" / "run_dim_tenant.json.gz",
                archived_paths,
            )
            self.assertIn(
                log_root / "_archive" / "2025-09-01" / "old.json.gz",
                archived_paths,
            )
            archive_path = (
                log_root / "_archive" / "2025-10-01" / "run_dim_tenant.json.gz"
            )
            with gzip.open(archive_path, "rt", encoding="utf-8") as archived_file:
                self.assertEqual(archived_file.read(), "{}\n")

            audit_logs = list((log_root / "_retention").glob("*.json"))
            self.assertEqual(len(audit_logs), 1)
            audit = json.loads(audit_logs[0].read_text(encoding="utf-8"))
            self.assertEqual(audit["operational_days"], 90)
            self.assertEqual(audit["audit_days"], 365)
            self.assertEqual(len(audit["archived"]), 2)
            self.assertEqual(len(audit["deleted"]), 2)

            quarantine_plan = apply_retention(
                log_root,
                quarantine_root,
                operational_days=90,
                audit_days=365,
                apply=False,
                now=now,
                timezone=timezone.utc,
                include_quarantine=True,
            )
            self.assertEqual(
                {candidate.path for candidate in quarantine_plan.candidates},
                {old_quarantine},
            )
            quarantine_apply = apply_retention(
                log_root,
                quarantine_root,
                operational_days=90,
                audit_days=365,
                apply=True,
                now=now,
                timezone=timezone.utc,
                include_quarantine=True,
            )
            self.assertEqual(quarantine_apply.archived, [])
            self.assertEqual(len(quarantine_apply.deleted), 1)
            self.assertFalse(old_quarantine.exists())
            self.assertTrue(old_quarantine_inside_audit.exists())

    def test_logs_only_mode_leaves_summaries_and_quarantine(self) -> None:
        now = datetime(2026, 10, 18, 2, 0, tzinfo=timezone.utc)
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary)
            log_root = base / "logs" / "spartment-transformation"
            quarantine_root = base / "quarantine" / "transformation"
            old_table = _file(log_root / "2025-09-01" / "run_dim_room.json", 400, now)
            old_summary = _file(log_root / "2025-09-01" / "run.json", 400, now)
            old_quarantine = _file(
                quarantine_root / "2025-09-01" / "dim_room_rejected.csv", 400, now
            )

            applied = apply_retention(
                log_root,
                quarantine_root,
                operational_days=90,
                audit_days=365,
                apply=True,
                now=now,
                timezone=timezone.utc,
                include_summaries=False,
            )

            self.assertEqual(len(applied.deleted), 1)
            self.assertFalse(old_table.exists())
            self.assertTrue(old_summary.exists())
            self.assertTrue(old_quarantine.exists())


if __name__ == "__main__":
    unittest.main()

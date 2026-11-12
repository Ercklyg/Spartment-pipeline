"""PostgreSQL integration tests; opt in with SPARTMENT_TEST_DSN (disposable DB)."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))
import psycopg
from psycopg import sql

from test_stage2 import base_rows, write_batch, settings as transform_settings, PARENT
from spartment.stage2_transform.pipeline import transform
from spartment.stage3_load.contract import OUTPUT_TABLES, LOCK_ID
from spartment.stage3_load.models import Settings
from spartment.stage3_load.package import parse_value
from spartment.stage3_load.pipeline import load
from spartment.stage3_load.schema import initialize, provision
from spartment.stage3_load.backup import restore
from spartment.stage3_load.recovery import reconcile
from spartment.stage3_load.retention import retain

TEST_DSN = os.environ.get("SPARTMENT_TEST_DSN")
PG_BIN = Path(os.environ.get("SPARTMENT_PG_BIN", "C:/Program Files/PostgreSQL/18/bin"))


class ParsingTests(unittest.TestCase):
    def test_empty_text_preserved_and_nullable_typed_null(self):
        self.assertEqual(parse_value("fact_payment", "reference_note", ""), "")
        self.assertIsNone(parse_value("fact_inquiry", "room_key", ""))

    def test_precision_nonfinite_boolean_and_overflow_rejected(self):
        for value in ("1.001", "NaN", "Infinity", "10000000000000000"):
            with self.assertRaises(ValueError):
                parse_value("fact_payment", "amount", value)
        with self.assertRaises(ValueError):
            parse_value("dim_date", "is_weekend", "yes")
        with self.assertRaises(ValueError):
            parse_value("dim_date", "year", "2147483648")


@unittest.skipUnless(TEST_DSN, "Set SPARTMENT_TEST_DSN to a disposable PostgreSQL database")
class LoadIntegrationTests(unittest.TestCase):
    def setUp(self):
        from psycopg.conninfo import conninfo_to_dict
        if not conninfo_to_dict(TEST_DSN).get("dbname", "").startswith("stage3_test"):
            self.fail("Integration tests require a disposable database named stage3_test*")
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            # Tests explicitly require a disposable database; never point at activity data.
            for schema in ("warehouse", "load_control"):
                c.execute(sql.SQL("DROP SCHEMA IF EXISTS {} CASCADE").format(sql.Identifier(schema)))
        initialize(TEST_DSN)
        self.settings = Settings(dsn=TEST_DSN, root=self.root, pg_bin=PG_BIN, warning_free_ratio=0)
        self.path = self.batch("first")

    def batch(self, suffix, rows=None):
        parent = PARENT + "-" + suffix
        write_batch(self.root, rows or base_rows(), parent=parent)
        result = transform(transform_settings(self.root), f"spartment-transform-2026-10-18-{suffix}", parent)
        self.assertEqual(result.decision, "APPROVED")
        return result.handover_path

    def counts(self):
        with psycopg.connect(TEST_DSN) as c:
            return {t: c.execute(sql.SQL("SELECT count(*) FROM warehouse.{}").format(sql.Identifier(t))).fetchone()[0] for t in OUTPUT_TABLES}

    def current(self):
        with psycopg.connect(TEST_DSN) as c:
            return c.execute("SELECT batch_id FROM load_control.current_snapshot").fetchone()

    def test_success_duplicate_and_dry_run(self):
        first = load(self.settings, self.path, dry_run=True)
        self.assertEqual(first["status"], "DRY_RUN", first)
        self.assertIsNone(self.current())
        result = load(self.settings, self.path)
        self.assertEqual(result["status"], "SUCCESS", result)
        self.assertEqual(len(result["tables"]), 13)
        self.assertEqual(self.counts(), {t: r["records_received"] for t, r in result["tables"].items()})
        counts = self.counts()
        duplicate = load(self.settings, self.path)
        self.assertEqual(duplicate["status"], "SKIPPED_ALREADY_LOADED", duplicate)
        self.assertEqual(self.counts(), counts)
        self.assertEqual(duplicate["original_success_run_id"], result["run_id"])

    def test_failed_replacement_rolls_back_every_table(self):
        good = load(self.settings, self.path)
        self.assertEqual(good["status"], "SUCCESS", good)
        counts, current = self.counts(), self.current()
        path = self.batch("second")
        def failure(step, conn):
            if step == "inserted:fact_billing":
                conn.execute("SELECT 1/0")
        failed = load(self.settings, path, hook=failure)
        self.assertEqual(failed["status"], "FAILED", failed)
        self.assertEqual(failed["error_code"], "22012")
        self.assertEqual(self.counts(), counts)
        self.assertEqual(self.current(), current)
        self.assertTrue(all(t["records_loaded"] == 0 for t in failed["tables"].values()))
        self.assertTrue(Path(failed["backup_path"]).is_file())
        recovered = load(self.settings, path)
        self.assertEqual(recovered["status"], "SUCCESS", recovered)

    def test_corrupt_and_unapproved_input_blocked(self):
        h = json.loads(self.path.read_text())
        file = self.root / "transformed" / "fact_payment" / "2026-10-18" / "fact_payment_2026-10-18.csv"
        file.write_bytes(file.read_bytes() + b"\n")
        result = load(self.settings, self.path)
        self.assertEqual(result["error_code"], "CHECKSUM", result)
        self.assertIsNone(self.current())
        h["acceptance_decision"]["decision"] = "MANUAL REVIEW"
        h["released_for_load"] = True
        self.path.write_text(json.dumps(h))
        result = load(self.settings, self.path)
        self.assertEqual(result["error_code"], "UPSTREAM_APPROVAL")

    def test_warning_hold_review_and_revalidation(self):
        warning_settings = replace(self.settings, warning_free_ratio=1.0)
        held = load(warning_settings, self.path)
        self.assertEqual(held["status"], "HELD", held)
        self.assertIsNone(self.current())
        h = json.loads(self.path.read_text())
        review = dict(batch_id=h["batch_id"], output_checksums=h["output_checksums"],
                      waived_checks=["PRE-12"], reviewed_by="activity-maintainer",
                      reviewed_at=datetime.now(timezone.utc).isoformat(), review_reason="Test resource warning disposition")
        result = load(warning_settings, self.path, warning_review=review)
        self.assertEqual(result["status"], "SUCCESS", result)
        self.assertIn("warning_review", result)

    def test_schema_drift_and_storage_failure(self):
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            c.execute("ALTER TABLE warehouse.fact_payment ADD COLUMN unexpected text")
        result = load(self.settings, self.path)
        self.assertEqual(result["error_code"], "TARGET_SCHEMA", result)

    def test_insufficient_space(self):
        result = load(replace(self.settings, min_free_bytes=10**20), self.path)
        self.assertEqual(result["error_code"], "PRE-08", result)
        self.assertIsNone(self.current())

    def test_updated_snapshot_replaces_values_without_inflating_totals(self):
        first = load(self.settings, self.path)
        self.assertEqual(first["status"], "SUCCESS", first)
        rows = base_rows()
        rows["payments"][0]["reference_note"] = "Changed approved reference"
        result = load(self.settings, self.batch("changed", rows))
        self.assertEqual(result["status"], "SUCCESS", result)
        with psycopg.connect(TEST_DSN) as c:
            note, count, amount = c.execute("SELECT min(reference_note),count(*),sum(amount) FROM warehouse.fact_payment").fetchone()
            self.assertEqual(note, "Changed approved reference")
            self.assertEqual(count, 1)
            self.assertEqual(str(amount), "8700.00")

    def test_stale_reference_date_rejected(self):
        self.assertEqual(load(self.settings, self.path)["status"], "SUCCESS")
        path = self.batch("stale")
        h = json.loads(path.read_text())
        h["reference_date"] = "2026-10-17"
        path.write_text(json.dumps(h))
        summary_path = self.root / "logs" / "spartment-transformation" / "2026-10-18" / f"{h['run_id']}.json"
        summary = json.loads(summary_path.read_text())
        summary["reference_date"] = h["reference_date"]
        summary_path.write_text(json.dumps(summary))
        result = load(self.settings, path)
        self.assertEqual(result["error_code"], "PRE-10", result)

    def test_post_load_mismatch_rolls_back(self):
        def tamper(step, conn):
            if step == "inserted:fact_payment":
                conn.execute("UPDATE warehouse.fact_payment SET amount=amount+1")
        result = load(self.settings, self.path, hook=tamper)
        self.assertEqual(result["status"], "FAILED", result)
        self.assertTrue(result["error_code"].startswith("POST-07"))
        self.assertIsNone(self.current())

    def test_constraint_failure_rolls_back(self):
        def invalid(step, conn):
            if step == "inserted:fact_payment":
                conn.execute("UPDATE warehouse.fact_payment SET amount=-1")
        result = load(self.settings, self.path, hook=invalid)
        self.assertEqual(result["error_code"], "23514", result)
        self.assertIsNone(self.current())

    def test_unavailable_connection_leaves_local_failure_evidence(self):
        result = load(replace(self.settings, dsn="host=127.0.0.1 port=1 dbname=stage3_tests connect_timeout=1"), self.path)
        self.assertEqual(result["status"], "FAILED")
        self.assertTrue(list((self.root / "loaded" / "_handover").glob("*.json")))

    def test_concurrent_writer_guard(self):
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            c.execute("SELECT pg_advisory_lock(%s)", (LOCK_ID,))
            result = load(replace(self.settings, lock_seconds=0), self.path)
            self.assertEqual(result["error_code"], "LOCK_TIMEOUT", result)
            c.execute("SELECT pg_advisory_unlock(%s)", (LOCK_ID,))

    def test_backup_restore_and_recovery(self):
        first = load(self.settings, self.path)
        self.assertEqual(first["status"], "SUCCESS", first)
        second = load(self.settings, self.batch("second"))
        self.assertEqual(second["status"], "SUCCESS", second)
        from psycopg.conninfo import conninfo_to_dict, make_conninfo
        params = conninfo_to_dict(TEST_DSN)
        db = "stage3_restore_" + str(os.getpid())
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            c.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db)))
        params["dbname"] = db
        restored_dsn = make_conninfo(**params)
        try:
            restore(restored_dsn, Path(second["backup_path"]), PG_BIN)
            with psycopg.connect(restored_dsn) as c:
                self.assertEqual(c.execute("SELECT batch_id FROM load_control.current_snapshot").fetchone()[0], first["batch_id"])
                self.assertEqual(c.execute("SELECT count(*) FROM warehouse.fact_payment").fetchone()[0], 1)
        finally:
            with psycopg.connect(TEST_DSN, autocommit=True) as c:
                c.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(db)))
        recovered = reconcile(self.settings, second["run_id"])
        self.assertEqual(recovered["status"], "SUCCESS")

    def test_runtime_role_is_restricted(self):
        with psycopg.connect(TEST_DSN, autocommit=True) as c:
            if c.execute("SELECT 1 FROM pg_roles WHERE rolname='spartment_loader'").fetchone():
                self.skipTest("Role provisioning tested once per isolated cluster")
        runtime = provision(TEST_DSN, self.root / ".env")
        result = load(replace(self.settings, dsn=runtime, storage_paths=(self.root,)), self.path)
        self.assertEqual(result["status"], "SUCCESS", result)
        with psycopg.connect(runtime, autocommit=True) as c:
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute("DELETE FROM load_control.loaded_batch")
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                c.execute("ALTER TABLE warehouse.fact_payment ADD COLUMN forbidden text")


class RetentionTests(unittest.TestCase):
    def test_retention_archive_and_incident_hold(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            folder = root / "logs" / "spartment-load" / "2026-01-01"
            folder.mkdir(parents=True)
            path = folder / "spartment-load-test_fact_payment.json"
            path.write_text(json.dumps({"run_id": "spartment-load-test", "table_name": "fact_payment"}))
            old = (datetime.now(timezone.utc) - timedelta(days=400)).timestamp()
            os.utime(path, (old, old))
            self.assertEqual(len(retain(root)["archived"]), 1)
            self.assertTrue(path.exists())
            holds = root / "loaded" / "_holds"
            holds.mkdir(parents=True)
            (holds / "spartment-load-test.json").write_text('{}')
            self.assertEqual(len(retain(root, apply=True)["protected"]), 1)
            self.assertTrue(path.exists())


if __name__ == "__main__":
    unittest.main()

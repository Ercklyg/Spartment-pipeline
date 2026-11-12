"""Integration against an exact schema-only replica of the populated Supabase target."""
from __future__ import annotations

import json
import os
import sys
import tempfile
import unittest
from unittest.mock import patch
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.types.json import Jsonb

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from test_stage2 import base_rows, write_batch, settings as transform_settings, PARENT
from spartment.stage2_transform.pipeline import transform
from spartment.stage3_load.backend import ExistingWarehouse, existing_contract, existing_catalog
from spartment.stage3_load.models import Settings
from spartment.stage3_load.pipeline import load
from spartment.stage3_load.recovery import reconcile
from spartment.stage3_load.hosted import validate_destination
from spartment.stage3_load.hosted import provision_existing

TEST_DSN = os.environ.get("SPARTMENT_TEST_DSN")


@unittest.skipUnless(TEST_DSN, "Use the disposable PostgreSQL runner")
class ExistingWarehouseTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        params = conninfo_to_dict(TEST_DSN)
        if not params.get("dbname", "").startswith("stage3_test"):
            raise RuntimeError("Disposable stage3_test* database required")
        cls.database = params["dbname"] + "_adapter"
        with psycopg.connect(TEST_DSN, autocommit=True) as conn:
            conn.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(cls.database)))
        params["dbname"] = cls.database
        cls.dsn = make_conninfo(**params)

    @classmethod
    def tearDownClass(cls):
        with psycopg.connect(TEST_DSN, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(cls.database)))

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        ddl = (ROOT / "warehouse" / "supabase-existing.sql").read_text()
        ddl = "\n".join(line for line in ddl.splitlines() if not line.startswith("\\"))
        expected = existing_contract()
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute("DROP SCHEMA IF EXISTS warehouse CASCADE")
            conn.execute("DROP SCHEMA IF EXISTS warehouse_meta CASCADE")
            conn.execute(ddl, prepare=False)
            actual = existing_catalog(conn)
            # PostgreSQL 18 re-renders the restored PG17 array CHECK casts.
            # Do not weaken production validation to accommodate a test server.
            self.assertEqual(actual["columns"], expected["catalog"]["columns"])
            self.assertEqual(actual["indexes"], expected["catalog"]["indexes"])
            self.assertEqual([{k:v for k,v in c.items() if k != "definition"} for c in actual["constraints"]],
                             [{k:v for k,v in c.items() if k != "definition"} for c in expected["catalog"]["constraints"]])
            expected["catalog"] = actual
            self.contract_patch = patch("spartment.stage3_load.backend.existing_contract", return_value=expected)
            self.contract_patch.start()
            self.addCleanup(self.contract_patch.stop)
            conn.execute("INSERT INTO warehouse_meta.schema_contract VALUES (%s,%s,%s)",
                         (expected["version"], expected["fingerprint"], Jsonb(actual)))
            ExistingWarehouse().validate(conn)
        self.settings = Settings(dsn=self.dsn, root=self.root, target_profile="supabase_existing",
                                 warning_free_ratio=0, pg_bin=Path(os.environ["SPARTMENT_PG_BIN"]))
        self.path = self.batch("first")

    def batch(self, suffix, rows=None):
        parent = PARENT + "-" + suffix
        write_batch(self.root, rows or base_rows(), parent=parent)
        outcome = transform(transform_settings(self.root), "spartment-transform-2026-10-18-" + suffix, parent)
        self.assertEqual(outcome.decision, "APPROVED")
        return outcome.handover_path

    def current(self):
        with psycopg.connect(self.dsn) as conn:
            return conn.execute("SELECT batch_id FROM warehouse_meta.current_batch").fetchone()

    def test_success_duplicate_and_existing_lineage(self):
        dry = load(self.settings, self.path, dry_run=True)
        self.assertEqual(dry["status"], "DRY_RUN", dry)
        self.assertIsNone(self.current())
        first = load(self.settings, self.path)
        self.assertEqual(first["status"], "SUCCESS", first)
        duplicate = load(self.settings, self.path)
        self.assertEqual(duplicate["status"], "SKIPPED_ALREADY_LOADED", duplicate)
        with psycopg.connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*),sum(amount) FROM warehouse.fact_payment").fetchone()[0], 1)
            self.assertIsNone(conn.execute("SELECT to_regnamespace('load_control')").fetchone()[0])
            self.assertEqual(conn.execute("SELECT load_batch_id FROM warehouse.fact_payment").fetchone()[0], first["batch_id"])
        self.assertEqual(reconcile(self.settings, first["run_id"])["status"], "SUCCESS")

    def test_rollback_preserves_existing_snapshot_and_history(self):
        first = load(self.settings, self.path)
        self.assertEqual(first["status"], "SUCCESS", first)
        def fault(step, conn):
            if step == "inserted:fact_billing":
                conn.execute("SELECT 1/0")
        result = load(self.settings, self.batch("second"), hook=fault)
        self.assertEqual(result["status"], "FAILED", result)
        self.assertEqual(self.current(), (first["batch_id"],))
        with psycopg.connect(self.dsn) as conn:
            self.assertEqual(conn.execute("SELECT count(*) FROM warehouse_meta.batches").fetchone()[0], 1)
            self.assertEqual(conn.execute("SELECT count(*) FROM warehouse.fact_billing").fetchone()[0], 2)

    def test_target_width_rejected_before_copy(self):
        rows = base_rows()
        rows["buildings"][0]["name"] = "X" * 201
        result = load(self.settings, self.batch("wide", rows))
        self.assertEqual(result.get("error_code"), "TARGET_WIDTH", result)
        self.assertIsNone(self.current())

    def test_legacy_fingerprint_is_not_assumed_to_match(self):
        h = json.loads(self.path.read_text())
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            conn.execute("""INSERT INTO warehouse_meta.batches
                (batch_id,transformation_run_id,extraction_run_id,processing_period,snapshot_end,fingerprint,run_id,report)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""", (h["batch_id"],h["run_id"],h["parent_run_id"],h["processing_period"],h["batch_window_end"],"legacy", "legacy-run",Jsonb({})))
        result = load(self.settings, self.path)
        self.assertEqual(result.get("error_code"), "LEGACY_BATCH_EVIDENCE", result)
        self.assertIsNone(self.current())

    def test_hosted_capacity_requires_evidence_or_review(self):
        hosted = replace(self.settings, capacity_mode="hosted")
        held = load(hosted, self.path)
        self.assertEqual(held["status"], "HELD", held)
        self.assertIsNone(self.current())
        path = self.root / "capacity.json"
        path.write_text(json.dumps({"project_ref":"local-test", "source":"isolated test evidence",
                                    "observed_at":datetime.now(timezone.utc).isoformat(),
                                    "available_disk_bytes":1024**3}))
        result = load(replace(hosted, capacity_evidence=path), self.path)
        self.assertEqual(result["status"], "SUCCESS", result)

    def test_hosted_quota_shortfall_is_critical(self):
        result = load(replace(self.settings, capacity_mode="hosted", database_budget_bytes=1), self.path)
        self.assertEqual(result.get("error_code"), "PRE-08", result)
        self.assertIsNone(self.current())

    def test_new_runtime_role_preserves_existing_contract(self):
        # Permit only this isolated test connection through the public CLI guard.
        with patch("spartment.stage3_load.hosted.validate_destination", return_value=conninfo_to_dict(self.dsn)):
            runtime = provision_existing(self.dsn, self.root / ".env", role="stage3_adapter_loader")
        result = load(replace(self.settings, dsn=runtime, storage_paths=(self.root,)), self.path)
        self.assertEqual(result["status"], "SUCCESS", result)
        with psycopg.connect(runtime, autocommit=True) as conn:
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                conn.execute("DELETE FROM warehouse_meta.batches")
            with self.assertRaises(psycopg.errors.InsufficientPrivilege):
                conn.execute("ALTER TABLE warehouse.dim_date ADD COLUMN forbidden text")


class HostedConfigTests(unittest.TestCase):
    def test_accepts_session_pooler_and_rejects_transaction_pooler(self):
        base = "host=aws-0-region.pooler.supabase.com user=postgres.project dbname=postgres sslmode=require"
        self.assertEqual(validate_destination(base + " port=5432")["dbname"], "postgres")
        with self.assertRaises(Exception):
            validate_destination(base + " port=6543")
        with self.assertRaises(Exception):
            validate_destination(base.replace("sslmode=require", "sslmode=disable"))

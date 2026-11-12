"""Metadata adapters: local reference schema and the existing Supabase warehouse."""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from spartment.common.config import REPO_ROOT
from .contract import LINEAGE, OUTPUT_COLUMNS, OUTPUT_TABLES, SCHEMA_VERSION
from .models import LoadError
from .schema import check_schema


def selected_backend(conn, profile: str = "auto"):
    existing = conn.execute("SELECT to_regclass('warehouse_meta.schema_contract')").fetchone()[0]
    if profile == "supabase_existing" or (profile == "auto" and existing):
        return ExistingWarehouse()
    if profile not in {"auto", "local"}:
        raise LoadError("CONFIG", "Unknown target profile")
    return LocalWarehouse()


class LocalWarehouse:
    profile = "local"
    schema_version = SCHEMA_VERSION
    metadata_schema = "load_control"
    lineage = LINEAGE

    def validate(self, conn):
        check_schema(conn)

    def validate_values(self, package):
        pass

    def audit(self, conn, result):
        with conn.transaction():
            conn.execute("""INSERT INTO load_control.load_attempt
                (run_id,batch_id,transformation_run_id,status,started_at,ended_at,detail)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT(run_id) DO UPDATE SET status=excluded.status,ended_at=excluded.ended_at,detail=excluded.detail""",
                (result["run_id"], result.get("batch_id"), result.get("transformation_run_id"), result["status"],
                 result["load_started_at"], result.get("load_ended_at"), Jsonb(result)))
            for table, detail in result["tables"].items():
                conn.execute("""INSERT INTO load_control.load_table_result VALUES (%s,%s,%s)
                    ON CONFLICT(run_id,table_name) DO UPDATE SET detail=excluded.detail""", (result["run_id"], table, Jsonb(detail)))

    def duplicate(self, execute, h):
        row = execute("SELECT run_id,checksums,batch_id,transformation_run_id FROM load_control.loaded_batch WHERE batch_id=%s OR transformation_run_id=%s",
                      (h["batch_id"], h["run_id"])).fetchone()
        if row and row[1:] != (h["output_checksums"], h["batch_id"], h["run_id"]):
            raise LoadError("BATCH_IDENTITY", "Committed batch identity or checksums differ")
        return row[0] if row else None

    def current(self, execute):
        return execute("""SELECT b.batch_id,b.processing_period,b.reference_date FROM load_control.current_snapshot s
            JOIN load_control.loaded_batch b USING(batch_id)""").fetchone()

    def fresh(self, prior, h):
        return prior is None or (date.fromisoformat(h["processing_period"]) >= prior[1]
                                 and date.fromisoformat(h["reference_date"]) >= prior[2])

    def insert_lineage(self, run_id, h, started):
        return self.lineage, (run_id, h["batch_id"], started)

    def publish(self, execute, h, result, counts):
        execute("INSERT INTO load_control.loaded_batch VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (h["batch_id"], h["run_id"], result["run_id"], h["processing_period"], h["reference_date"],
                 result["published_at"], Jsonb(h["output_checksums"]), Jsonb(counts)))
        execute("INSERT INTO load_control.current_snapshot VALUES (true,%s) ON CONFLICT(singleton) DO UPDATE SET batch_id=excluded.batch_id", (h["batch_id"],))

    def committed(self, execute, batch_id):
        row = execute("SELECT run_id FROM load_control.loaded_batch WHERE batch_id=%s", (batch_id,)).fetchone()
        return row[0] if row else None


def existing_catalog(conn):
    """Match the live warehouse's registered catalog format, including widths/scales."""
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute("""SELECT table_schema,table_name,column_name,ordinal_position,data_type,udt_name,
            is_nullable,column_default,character_maximum_length,numeric_precision,numeric_scale
            FROM information_schema.columns WHERE table_schema IN ('warehouse','warehouse_meta')
            ORDER BY table_schema,table_name,ordinal_position""")
        columns = cur.fetchall()
        cur.execute("""SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes
            WHERE schemaname IN ('warehouse','warehouse_meta') ORDER BY schemaname,tablename,indexname""")
        indexes = cur.fetchall()
        cur.execute("""SELECT n.nspname AS schema,t.relname AS table_name,k.conname AS name,
            pg_get_constraintdef(k.oid) AS definition,k.convalidated AS validated
            FROM pg_constraint k JOIN pg_class t ON t.oid=k.conrelid
            JOIN pg_namespace n ON n.oid=t.relnamespace
            WHERE n.nspname IN ('warehouse','warehouse_meta') AND k.contype IN ('p','u','f','c','x')
            ORDER BY n.nspname,t.relname,k.conname""")
        constraints = cur.fetchall()
    return dict(columns=columns, indexes=indexes, constraints=constraints)


def existing_contract():
    path = REPO_ROOT / "warehouse" / "supabase-existing-contract.json"
    return json.loads(path.read_text(encoding="utf-8"))


class ExistingWarehouse(LocalWarehouse):
    profile = "supabase_existing"
    schema_version = "1.0"
    metadata_schema = "warehouse_meta"
    lineage = ("load_batch_id",)

    def validate(self, conn):
        expected = existing_contract()
        rows = conn.execute("SELECT version,fingerprint,catalog FROM warehouse_meta.schema_contract").fetchall()
        if rows != [(expected["version"], expected["fingerprint"], expected["catalog"])]:
            raise LoadError("TARGET_SCHEMA", "Registered warehouse_meta contract differs from the reviewed Supabase contract")
        if existing_catalog(conn) != expected["catalog"]:
            raise LoadError("TARGET_SCHEMA", "Existing Supabase columns, constraints or indexes drifted")
        if conn.execute("""SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT t.tgisinternal
            AND n.nspname IN ('warehouse','warehouse_meta')""").fetchone()[0]:
            raise LoadError("TARGET_TRIGGER", "Unreviewed target trigger could alter incoming values")

    def validate_values(self, package):
        # Avoid server-side VARCHAR truncation of excess spaces and NUMERIC rounding.
        columns = {(c["table_name"], c["column_name"]): c for c in existing_contract()["catalog"]["columns"]}
        for table in OUTPUT_TABLES:
            for number, row in enumerate(package.rows[table], 2):
                for name, value in zip(OUTPUT_COLUMNS[table], row):
                    if value is None:
                        continue
                    spec = columns[table, name]
                    length = spec["character_maximum_length"]
                    if length is not None and (len(str(value)) > length or
                            (spec["udt_name"] == "bpchar" and len(str(value)) != length)):
                        raise LoadError("TARGET_WIDTH", f"Target width mismatch: {table}.{name}, line {number}")
                    if spec["udt_name"] == "int2" and not -32768 <= value <= 32767:
                        raise LoadError("TARGET_RANGE", f"Target smallint overflow: {table}.{name}, line {number}")
                    if spec["udt_name"] == "numeric" and spec["numeric_scale"] is not None:
                        decimal = Decimal(value)
                        scale, precision = spec["numeric_scale"], spec["numeric_precision"]
                        if decimal != decimal.quantize(Decimal(1).scaleb(-scale)) or abs(decimal) >= Decimal(10) ** (precision-scale):
                            raise LoadError("TARGET_PRECISION", f"Target precision loss: {table}.{name}, line {number}")

    def audit(self, conn, result):
        conn.execute("""INSERT INTO warehouse_meta.attempts(run_id,batch_id,status,report)
            VALUES (%s,%s,%s,%s) ON CONFLICT(run_id) DO UPDATE
            SET status=excluded.status,report=excluded.report,recorded_at=now()""",
            (result["run_id"], result.get("batch_id"), result["status"], Jsonb(result)))

    def duplicate(self, execute, h):
        row = execute("""SELECT run_id,batch_id,transformation_run_id,report FROM warehouse_meta.batches
            WHERE batch_id=%s OR transformation_run_id=%s""", (h["batch_id"], h["run_id"])).fetchone()
        if row is None:
            return None
        if row[1:3] != (h["batch_id"], h["run_id"]):
            raise LoadError("BATCH_IDENTITY", "Existing batch or transformation identity conflicts")
        if not row[3].get("output_checksums"):
            raise LoadError("LEGACY_BATCH_EVIDENCE", "Legacy batch has no file checksums; replay is refused without provable artifact identity")
        if row[3]["output_checksums"] != h["output_checksums"]:
            raise LoadError("BATCH_IDENTITY", "Existing batch file checksums differ")
        return row[0]

    def current(self, execute):
        return execute("""SELECT b.batch_id,b.processing_period,b.snapshot_end FROM warehouse_meta.current_batch s
            JOIN warehouse_meta.batches b USING(batch_id)""").fetchone()

    def fresh(self, prior, h):
        incoming = datetime.fromisoformat(h["batch_window_end"])
        if incoming.tzinfo is None:
            raise LoadError("INPUT_WINDOW", "Snapshot end must have a timezone")
        return prior is None or (date.fromisoformat(h["processing_period"]) >= prior[1] and incoming >= prior[2])

    def insert_lineage(self, run_id, h, started):
        return self.lineage, (h["batch_id"],)

    def publish(self, execute, h, result, counts):
        payload = {"scheme": "file-checksums-v1", "batch_id": h["batch_id"], "transformation_run_id": h["run_id"],
                   "output_checksums": h["output_checksums"]}
        digest = hashlib.sha256(json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        result["fingerprint"], result["fingerprint_scheme"] = digest, "file-checksums-v1"
        execute("""INSERT INTO warehouse_meta.batches
            (batch_id,transformation_run_id,extraction_run_id,processing_period,snapshot_end,fingerprint,run_id,report)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (h["batch_id"], h["run_id"], h["parent_run_id"], h["processing_period"], h["batch_window_end"], digest, result["run_id"], Jsonb(result)))
        execute("INSERT INTO warehouse_meta.current_batch VALUES (true,%s) ON CONFLICT(singleton) DO UPDATE SET batch_id=excluded.batch_id", (h["batch_id"],))
        # Batch FK is deferred in the reviewed schema; verify it before commit.
        execute("SET CONSTRAINTS ALL IMMEDIATE")

    def committed(self, execute, batch_id):
        row = execute("SELECT run_id FROM warehouse_meta.batches WHERE batch_id=%s", (batch_id,)).fetchone()
        return row[0] if row else None

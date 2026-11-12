"""Explicit, owner-only schema setup; the runtime never alters target objects."""
from __future__ import annotations

import hashlib
import json
import secrets
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from .contract import (CODE_DOMAINS, FK_COLUMNS, LINEAGE, NATURAL_KEY, NON_NEGATIVE,
                       OUTPUT_COLUMNS, OUTPUT_TABLES, REQUIRED_NOT_NULL,
                       SCHEMA_VERSION, fingerprint, primary_key, sql_type)
from .models import LoadError


def statements() -> list[str]:
    result = ["CREATE SCHEMA warehouse", "CREATE SCHEMA load_control"]
    result += [
        "CREATE TABLE load_control.schema_version (version text PRIMARY KEY, contract_hash text NOT NULL, catalog_hash text NOT NULL)",
        """CREATE TABLE load_control.load_attempt (
          run_id text PRIMARY KEY, batch_id text, transformation_run_id text,
          status text NOT NULL CHECK (status IN ('RUNNING','SUCCESS','HELD','FAILED',
            'COMMIT_UNKNOWN','SKIPPED_ALREADY_LOADED','INTERRUPTED','DRY_RUN')),
          started_at timestamptz NOT NULL, ended_at timestamptz, detail jsonb NOT NULL)""",
        """CREATE TABLE load_control.load_table_result (
          run_id text REFERENCES load_control.load_attempt(run_id), table_name text NOT NULL,
          detail jsonb NOT NULL, PRIMARY KEY(run_id,table_name))""",
        """CREATE TABLE load_control.loaded_batch (
          batch_id text PRIMARY KEY, transformation_run_id text UNIQUE NOT NULL,
          run_id text NOT NULL REFERENCES load_control.load_attempt(run_id),
          processing_period date NOT NULL, reference_date date NOT NULL,
          published_at timestamptz NOT NULL, checksums jsonb NOT NULL, counts jsonb NOT NULL)""",
        """CREATE TABLE load_control.current_snapshot (
          singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
          batch_id text NOT NULL REFERENCES load_control.loaded_batch(batch_id))""",
    ]
    for table in OUTPUT_TABLES:
        fields = []
        for c in OUTPUT_COLUMNS[table]:
            null = " NOT NULL" if c in REQUIRED_NOT_NULL[table] else ""
            fields.append(f'"{c}" {sql_type(table, c)}{null}')
        fields += ["load_run_id text NOT NULL REFERENCES load_control.load_attempt(run_id)",
                   "load_batch_id text NOT NULL", "loaded_at timestamptz NOT NULL"]
        fields += [f'PRIMARY KEY ("{primary_key(table)}")', f'UNIQUE ("{NATURAL_KEY[table]}")']
        for c, parent, _ in FK_COLUMNS.get(table, ()):
            fields.append(f'FOREIGN KEY ("{c}") REFERENCES warehouse."{parent}" ("{primary_key(parent)}")')
        for c in NON_NEGATIVE.get(table, ()):
            fields.append(f'CHECK ("{c}" >= 0)')
        for c, domain in CODE_DOMAINS.get(table, {}).items():
            allowed = ",".join("'" + value.replace("'", "''") + "'" for value in sorted(domain))
            fields.append(f'CHECK ("{c}" IN ({allowed}))')
        for c in OUTPUT_COLUMNS[table]:
            if sql_type(table, c) == "character varying(16)":
                fields.append(f'CHECK ("{c}" ~ \'^[0-9a-f]{{16}}$\')')
        if table == "dim_date":
            fields += ["CHECK (date_key = to_char(date, 'YYYYMMDD'))",
                       'CHECK (year = extract(year from date))',
                       'CHECK (quarter = extract(quarter from date))',
                       'CHECK (month = extract(month from date))',
                       'CHECK (week = extract(week from date))',
                       'CHECK (day_of_month = extract(day from date))',
                       'CHECK (day_of_week = extract(isodow from date))']
        result.append(f'CREATE TABLE warehouse."{table}" (\n  ' + ",\n  ".join(fields) + "\n)")
        for c in sorted({x[0] for x in FK_COLUMNS.get(table, ())} | {"load_batch_id"}):
            result.append(f'CREATE INDEX "{table}_{c}_idx" ON warehouse."{table}" ("{c}")')
    result += [
        "CREATE INDEX fact_billing_reporting_idx ON warehouse.fact_billing(tenant_key,status,due_date)",
        "CREATE INDEX fact_payment_reporting_idx ON warehouse.fact_payment(bill_key,payment_date)",
        """CREATE VIEW warehouse.snapshot_status AS
            SELECT b.batch_id,b.run_id,b.transformation_run_id,b.processing_period,
            b.reference_date,b.published_at,b.counts FROM load_control.current_snapshot s
            JOIN load_control.loaded_batch b USING(batch_id)""",
    ]
    return result


def catalog_hash(conn) -> str:
    """Hash actual columns/defaults, constraints and indexes, not just a version label."""
    columns = conn.execute("""SELECT n.nspname,c.relname,a.attname,
        format_type(a.atttypid,a.atttypmod),a.attnotnull,pg_get_expr(d.adbin,d.adrelid)
        FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid
        JOIN pg_namespace n ON n.oid=c.relnamespace
        LEFT JOIN pg_attrdef d ON d.adrelid=c.oid AND d.adnum=a.attnum
        WHERE n.nspname IN ('warehouse','load_control') AND c.relkind='r'
        AND a.attnum>0 AND NOT a.attisdropped ORDER BY 1,2,a.attnum""").fetchall()
    constraints = conn.execute("""SELECT n.nspname,c.relname,k.conname,
        pg_get_constraintdef(k.oid),k.convalidated FROM pg_constraint k
        JOIN pg_class c ON c.oid=k.conrelid JOIN pg_namespace n ON n.oid=c.relnamespace
        WHERE n.nspname IN ('warehouse','load_control') ORDER BY 1,2,3""").fetchall()
    indexes = conn.execute("""SELECT schemaname,tablename,indexname,indexdef FROM pg_indexes
        WHERE schemaname IN ('warehouse','load_control') ORDER BY 1,2,3""").fetchall()
    views = conn.execute("SELECT schemaname,viewname,definition FROM pg_views WHERE schemaname IN ('warehouse','load_control') ORDER BY 1,2").fetchall()
    return hashlib.sha256(json.dumps([columns, constraints, indexes, views], default=str).encode()).hexdigest()


def initialize(dsn: str) -> bool:
    with psycopg.connect(dsn, autocommit=True, connect_timeout=10) as conn:
        with conn.transaction():
            # Fail closed on existing unrecognized objects; never overwrite a database.
            if conn.execute("SELECT to_regclass('load_control.schema_version')").fetchone()[0]:
                check_schema(conn)
                return False
            for statement in statements():
                conn.execute(statement)
            conn.execute("INSERT INTO load_control.schema_version VALUES (%s,%s,%s)",
                         (SCHEMA_VERSION, fingerprint(), catalog_hash(conn)))
    return True


def check_schema(conn) -> None:
    try:
        row = conn.execute("SELECT version,contract_hash,catalog_hash FROM load_control.schema_version").fetchall()
    except psycopg.Error as exc:
        raise LoadError("TARGET_SCHEMA", "Initialize the warehouse schema with the owner connection first") from exc
    if row != [(SCHEMA_VERSION, fingerprint(), catalog_hash(conn))]:
        raise LoadError("TARGET_SCHEMA", "Target schema, constraints, indexes or contract version differ")


def provision(owner_dsn: str, env_file: Path) -> str:
    """Create the runtime login once; never reset an existing role's password."""
    role = "spartment_loader"
    reporter = "spartment_reporter"
    params = conninfo_to_dict(owner_dsn)
    with psycopg.connect(owner_dsn, autocommit=True) as conn:
        with conn.transaction():
            exists = conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (role,)).fetchone()
            if exists:
                raise LoadError("ROLE_EXISTS", "spartment_loader already exists; configure its existing credentials instead of resetting them")
            password = secrets.token_urlsafe(32)
            conn.execute(sql.SQL("CREATE ROLE {} LOGIN PASSWORD {}").format(sql.Identifier(role), sql.Literal(password)))
            if not conn.execute("SELECT 1 FROM pg_roles WHERE rolname=%s", (reporter,)).fetchone():
                conn.execute(sql.SQL("CREATE ROLE {} NOLOGIN").format(sql.Identifier(reporter)))
            database = conn.execute("SELECT current_database()").fetchone()[0]
            conn.execute(sql.SQL("GRANT CONNECT,TEMP ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(role)))
            conn.execute(sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(sql.Identifier(database), sql.Identifier(reporter)))
            conn.execute("GRANT USAGE ON SCHEMA warehouse,load_control TO spartment_loader")
            conn.execute("GRANT SELECT,INSERT,DELETE ON ALL TABLES IN SCHEMA warehouse TO spartment_loader")
            conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA load_control TO spartment_loader")
            conn.execute("GRANT INSERT,UPDATE ON load_control.load_attempt TO spartment_loader")
            conn.execute("GRANT INSERT,UPDATE ON load_control.load_table_result TO spartment_loader")
            conn.execute("GRANT INSERT ON load_control.loaded_batch TO spartment_loader")
            conn.execute("GRANT INSERT,UPDATE ON load_control.current_snapshot TO spartment_loader")
            conn.execute("GRANT USAGE ON SCHEMA warehouse TO spartment_reporter")
            conn.execute("GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO spartment_reporter")
            # Read the non-secret local path without granting server-file privileges.
            data_path = conn.execute("SHOW data_directory").fetchone()[0]
    params.update(user=role, password=password)
    runtime = make_conninfo(**params)
    text = env_file.read_text(encoding="utf-8") if env_file.exists() else ""
    lines = [line for line in text.splitlines() if not line.startswith(("SPARTMENT_LOAD_DSN=", "SPARTMENT_LOAD_STORAGE_PATHS="))]
    lines += ["SPARTMENT_LOAD_DSN=" + runtime, "SPARTMENT_LOAD_STORAGE_PATHS=" + data_path]
    env_file.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return runtime

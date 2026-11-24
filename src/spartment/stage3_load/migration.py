"""Explicit owner-only upgrade of the reviewed existing Supabase warehouse."""
import json

from psycopg.types.json import Jsonb

from spartment.common.config import REPO_ROOT
from .backend import existing_catalog, existing_contract
from .contract import LOCK_ID
from .models import LoadError


def migrate_announcements(conn, previous: dict, expected: dict) -> bool:
    """Fail closed on drift; update the single registered contract atomically."""
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout = '30s'")
        conn.execute("SELECT pg_advisory_xact_lock(%s)", (LOCK_ID,))
        conn.execute("LOCK TABLE warehouse.fact_notification IN ACCESS EXCLUSIVE MODE")
        conn.execute("LOCK TABLE warehouse_meta.schema_contract IN ACCESS EXCLUSIVE MODE")
        rows = conn.execute("SELECT version,fingerprint,catalog FROM warehouse_meta.schema_contract").fetchall()
        catalog = existing_catalog(conn)
        if conn.execute("""SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid
            JOIN pg_namespace n ON n.oid=c.relnamespace WHERE NOT t.tgisinternal
            AND n.nspname IN ('warehouse','warehouse_meta')""").fetchone()[0]:
            raise LoadError("TARGET_TRIGGER", "Unreviewed target trigger")
        if rows == [(expected['version'], expected['fingerprint'], expected['catalog'])] and catalog == expected['catalog']:
            return False
        if rows != [(previous['version'], previous['fingerprint'], previous['catalog'])] or catalog != previous['catalog']:
            raise LoadError("TARGET_SCHEMA", "Migration requires the exact reviewed previous contract")
        conn.execute("ALTER TABLE warehouse.fact_notification DROP CONSTRAINT fact_notification_check_0")
        # Use the reviewed definition verbatim: explicit array casts matter to
        # the catalog contract across supported PostgreSQL versions.
        definition = next(c['definition'] for c in expected['catalog']['constraints']
                          if c['schema'] == 'warehouse' and c['table_name'] == 'fact_notification'
                          and c['name'] == 'fact_notification_check_0')
        conn.execute("ALTER TABLE warehouse.fact_notification ADD CONSTRAINT fact_notification_check_0 " + definition)
        if existing_catalog(conn) != expected['catalog']:
            raise LoadError("TARGET_SCHEMA", "Migrated catalog differs from reviewed target; rolling back")
        conn.execute("UPDATE warehouse_meta.schema_contract SET version=%s,fingerprint=%s,catalog=%s",
                     (expected['version'], expected['fingerprint'], Jsonb(expected['catalog'])))
    return True


def reviewed_contracts():
    previous = json.loads((REPO_ROOT / 'warehouse' / 'supabase-existing-contract-v1.json').read_text())
    return previous, existing_contract()

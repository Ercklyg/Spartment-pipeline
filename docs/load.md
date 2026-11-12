# Spartment: Operational and Analytical Batch Data Pipeline

## Stage 3: Data Load Documentation

### Objective

Load one approved Stage 2 snapshot into the existing Supabase analytical warehouse. Verify completeness, accuracy and lineage before making the new data available to reports.

This document follows all nine sections of the supplied Stage 3 lab. The `supabase_existing` adapter in `src/spartment/stage3_load/` preserves the existing `warehouse` and `warehouse_meta` design. It does not recreate populated tables or reset the existing loader role. See [extraction.md](extraction.md), [transformation.md](transformation.md), and [load-verification.md](load-verification.md).

The laptop executes the batch scripts; Supabase hosts the destination. No streaming, micro-batch, new cleaning, re-extraction or business recalculation occurs in Load. A successful connection or isolated test is not evidence that an operational refresh or scheduled run occurred.

**Verified execution:** a real-source approved batch has now been backed up, reconciled and published successfully: 2,293 analytical rows. An identical replay was skipped without extra analytical writes. The full 55-test suite passed. See [load-verification.md](load-verification.md) for measured results and the separately recorded local-disk warning review. Scheduler activation is not claimed.

---

## 1. Load Source and Target Specification

### 1.1 Input and Identity

| Input | Existing path / field | Purpose |
| --- | --- | --- |
| Stage 2 handover | `transformed/_handover/<transformation_run_id>.json` | Exact batch, acceptance, release, paths and checksums |
| Thirteen UTF-8 CSVs | `transformed_datasets`; normally `transformed/<table>/<run_date>/<table>_<run_date>.csv` | Only analytical rows Load may persist |
| Validation | `transformed/_validation/<run_id>.json` | Matching APPROVED/PASS evidence |
| Run summary | `logs/spartment-transformation/<run_date>/<run_id>.json` | Batch/window/rules agreement and expected table counts |
| Table logs | Same directory, `<run_id>_<table>.json` | Corroborating `records_output` and file checksums |
| Quarantine references | Handover `quarantine_files` | Explain upstream exclusions; never loaded |

Freeze copies under `loaded/_work/<load_run_id>/` and verify the copied bytes against `output_checksums`. Stage 2 files are date-scoped and may be overwritten by another run, so Load reads only the verified copies.

| Attribute | Load rule |
| --- | --- |
| `run_id` | Transformation lineage, preserved in existing analytical columns |
| `parent_run_id` | Extraction lineage, stored as registry `extraction_run_id` |
| `batch_id` | Preserve the upstream ID; do not substitute the load attempt ID |
| `processing_period` | ISO snapshot processing date |
| `batch_window_start/end` | Preserve upstream boundaries; end becomes registry `snapshot_end` |
| `reference_date` | Preserve the business reference date; do not recalculate aging |
| `rules_version` | Supported Stage 2 contract required |
| `output_checksums`, `content_checksums` | File integrity and reproducibility; distinct values, both retained |

Require `acceptance_decision.decision == APPROVED` and `released_for_load == true`. Stage 2's manual-release flag cannot override the lab's APPROVED-only requirement. Metadata, validation, table logs, counts, headers, run IDs and checksums must agree. Correct defects upstream; never edit a handover to bypass approval.

### 1.2 Destination and Method

| Item | Specification |
| --- | --- |
| Destination | Existing Supabase warehouse project, PostgreSQL 17.11 at inspection |
| SQL database name | `postgres`; the project name is not the database name |
| Schemas | Published `warehouse`; existing metadata `warehouse_meta` |
| Connection | TLS direct PostgreSQL or shared Session pooler, port 5432 |
| Loader | Python 3.11+ and psycopg 3; optional project `load` extra |
| Transfer | Typed `COPY FROM STDIN` to temporary tables, explicit-column INSERT to targets |
| Load type | Full refresh of all 13 tables, one publication transaction |
| Temporary staging | Session-local, `ON COMMIT DROP` |
| Runtime location | Laptop; no paid scheduler/server required for this activity |
| Credentials | Owner/runtime DSNs in `.env`, excluded from Git; never in logs or command arguments |

Transaction pooling is rejected: temporary tables and advisory locks require session continuity. A custom role's shared-pooler username includes the project suffix. See [Supabase connection documentation](https://supabase.com/docs/guides/database/connecting-to-postgres).

### 1.3 Schedule, Volume and Resources

The optional Airflow Load DAG runs at 03:00 Asia/Manila, waiting for an approved handover for its logical processing date. Its window is 03:00-03:30. Warn after 20 minutes; roll back if the remaining 30-minute execution budget expires. Connection timeout is 10 seconds and lock timeout is 30 seconds.

The Windows Task Scheduler alternative starts the entire pipeline at 01:00: extraction, approved transformation, then Load. Its Load budget starts when Load begins. Choose one scheduling method. The laptop must be awake, online, and logged in for the Windows interactive task. Supabase hosting alone does not execute scripts; missed runs require manual recovery.

Batch size is all 13 approved CSVs. Expected count per table is `records_output`, verified against the file, not source `records_read`. Zero rows are accepted only when approved metadata agrees. Inspection observed 1,999 pre-existing warehouse rows; those are not new loader results. Every incoming batch records actual counts, bytes, staging size and duration.

| Resource / role | Required responsibility |
| --- | --- |
| Laptop/Python/network | Execute scripts, hold copies/logs/backups, maintain connection to Supabase |
| PostgreSQL client tools | `pg_dump` / `pg_restore`; a local server is needed only for disposable tests |
| Destination owner | Inspection, reviewed role provisioning and logical backups; no source API key substituted |
| `spartment_activity_loader` | Provisioned restricted runtime role: CONNECT/TEMP; schema USAGE; warehouse SELECT/INSERT/DELETE; metadata SELECT; attempts INSERT/UPDATE; batches INSERT; current_batch INSERT/UPDATE |
| Existing `spartment_loader` | Preserved; adaptation does not reset its password or grants |
| Maintainer / Super Admin | Review warnings, incidents, retention and restores; student may perform both roles |
| Reporting users | Appropriate SELECT-only access; no write or quarantine access implied |

The source Supabase URL and service-role key are Stage 1 settings and may belong to a different project. The target must be writable and have adequate configured capacity. The latest-snapshot warehouse does not provide SCD Type 2 history.

---

## 2. Target Tables and Column Specification

### 2.1 Verified Types and Constraints

The authoritative existing SQL/catalog are [supabase-existing.sql](../warehouse/supabase-existing.sql) and [supabase-existing-contract.json](../warehouse/supabase-existing-contract.json). They contain schema metadata, not tenant data. The adapter checks the registered and live catalog, including widths, numeric scales, nullability, defaults, PK/UQ/FK/CHECK constraints and indexes. Unreviewed user triggers are refused.

| Family | Existing type / handling |
| --- | --- |
| Surrogate and date keys | `character(16)` hexadecimal keys; `character(8)` YYYYMMDD date key; never regenerated |
| Natural IDs | UUID, unique and NOT NULL |
| Calendar/year/month fields | Smallint where declared; reject overflow |
| Other counts/durations | Integer as declared in the catalog |
| Money / rate / room area | `numeric(12,2)` / `numeric(9,4)` / `numeric(6,2)`; reject precision loss or overflow rather than round |
| Text/codes/contact | Existing text or bounded varchar; reject excess lengths, including spaces PostgreSQL might otherwise truncate |
| Dates/timestamps/booleans | Date, timestamptz, boolean; preserve supplied values/instant; no guessed representations |
| JSON-like audit payloads | Text as supplied by Stage 2; do not introduce a jsonb business conversion |

Empty nullable typed fields become SQL NULL; optional empty text stays empty text because the upstream CSV cannot distinguish missing text from an empty string. Required values cannot be absent. No analytical defaults fill missing input. Column lists come from Stage 2 `OUTPUT_COLUMNS`.

Every target has only `load_batch_id` beyond Stage 2's columns. It references `warehouse_meta.batches(batch_id)` through a DEFERRABLE INITIALLY DEFERRED FK. Do not add the local implementation's `load_run_id` or `loaded_at`; resolve them through the registry/report. `dim_date` also resolves transformation lineage through its batch.

### 2.2 Tables and Dependency Order

Every listed PK is NOT NULL/unique; each natural key has a separate UNIQUE constraint. All tables are in `warehouse` and have the batch FK above.

| Order / table | Type / reason for inclusion | PK / natural match key | Important columns / types | Parent relationships |
| --- | --- | --- | --- | --- |
| 1. `dim_date` | Dimension; calendar/month grouping | date_key / date | Date; calendar smallints; period char(7); weekend boolean | Batch only |
| 2. `dim_building` | Dimension; building and occupancy rollups | building_key / building_id | Name varchar(200); counts integer; occupancy numeric(9,4); vacancy numeric(12,2) | Batch only |
| 3. `dim_room` | Dimension; availability/location/rate | room_key / room_id | Room varchar(50); floor smallint; area numeric(6,2); rate numeric(12,2); flags boolean | Building |
| 4. `dim_tenant` | Dimension; unified tenant/staff identity | tenant_key / tenant_id | Name varchar(200); contact/role fields; rent numeric(12,2); dates; outstanding count integer | No reverse assignment FK |
| 5. `dim_lease` | Dimension; tenancy spine | lease_key / lease_id | Start/end dates; status varchar(20); lease_days integer; flags | Tenant, room |
| 6. `fact_assignment` | Fact; one accepted assignment | assignment_key / assignment_id | Rent numeric(12,2); move/end dates; tenure integer; status/active | Tenant; nullable room |
| 7. `fact_billing` | Fact; bill, payment rollup and aging | bill_key / bill_id | Components/total/paid/balance numeric(12,2); dates; period char(7); rate numeric(9,4); aging integers | Lease, tenant, room |
| 8. `fact_payment` | Fact; one cash receipt | payment_key / payment_id | Amount numeric(12,2); payment date; rank/count integers; reference text | Bill, lease, tenant |
| 9. `fact_payment_correction` | Fact; one correction audit event | correction_key / correction_id | Payment/actor UUIDs; before/after/reason text; timestamp; validity flag | Payment; no external auth FK |
| 10. `fact_inquiry` | Fact; prospective tenant inquiry | inquiry_key / inquiry_id | Name/contact/status fields; move date; age integer; open flag | Nullable room |
| 11. `fact_tenant_request` | Fact; maintenance/other request | request_key / request_id | Kind/subject/priority/status; timestamps; nullable resolution days | Tenant; nullable room |
| 12. `fact_occupancy_event` | Fact; move-out/transfer | event_key / event_id | Type varchar(20); details text; timestamp; validity flag | Tenant, performer tenant, previous lease, nullable new lease |
| 13. `fact_notification` | Fact; notification audit | notification_key / notification_id | Kind/title fields; nullable source UUID; read flag; timestamp | Tenant; source_id remains polymorphic |

Nullable references are preserved, including ended assignments without rooms and move-out events without new leases. No Unknown member is invented. Existing CHECK constraints enforce codes, monetary non-negativity, key/period formats and declared date relations. Existing unique/FK/reporting indexes stay enabled. No new partitions are justified for this activity.

### 2.3 Metadata Objects

| Existing object | Columns / purpose |
| --- | --- |
| warehouse_meta.attempts | Run PK, nullable batch, status, recorded_at default now(), JSON report with checks and table results |
| warehouse_meta.batches | Batch PK; unique transformation ID; extraction ID; processing date; snapshot_end; fingerprint; run; completed_at default now(); report |
| warehouse_meta.current_batch | Singleton boolean PK/check/default true; batch FK; publication pointer |
| warehouse_meta.schema_contract | Version PK, fingerprint and catalog JSON; runtime reads only |

No `load_control` schema is created on Supabase. The local reference schema is retained only for development/regression tests.

---

## 3. Load Strategy and Rules

| Targets | Load/write strategy | Changing data / match keys | Failure |
| --- | --- | --- | --- |
| Five dimensions | FULL replacement | Existing PK/natural keys; current attributes overwritten, equivalent to SCD Type 1 | Whole-batch rollback |
| Eight facts | FULL replacement | Existing fact keys; preserve supplied corrected values and events | Whole-batch rollback |

There are no SCD Type 2 effective-date/current-history columns. Preserve inactive rows present in the snapshot. Absence from a new full snapshot removes a row from publication but does not prove a source deletion. Late arrivals require a later approved upstream snapshot. Contract differences fail; Load does not alter tables or clean values.

### 3.1 Atomic Publication

1. Freeze and verify the package, connect, validate schema and actual width/scale/range compatibility.
2. Acquire the advisory lock, record the attempt, check duplicate identity and current processing date/snapshot end.
3. Begin one transaction. COPY all rows into temporary tables with the existing CHECK/unique constraints; verify incoming reference closure and capacity.
4. Dry run rolls back here. Unreviewed warnings hold publication without changing the prior snapshot.
5. Lock all warehouse tables in parent-first order and the current-batch registry. Recheck duplicate/current state to detect a concurrent legacy writer.
6. Download and verify a pre-refresh logical backup of warehouse and warehouse_meta while writer locks protect that snapshot.
7. DELETE children before parents, then INSERT parents before children in Section 2.2 order, adding only load_batch_id.
8. Reconcile all values/counts/money. Register the accepted batch and update current_batch in this transaction; force deferred constraints immediate.
9. Commit once after critical PASS and warning disposition. Confirm the durable registry, write completion evidence, release the lock.

Uncommitted changes remain invisible to ordinary readers. Multi-statement reports requiring one consistent snapshot should use a read-only repeatable-read transaction. Constraints are never disabled.

### 3.2 Idempotency and Recovery

New adapter reports store exact file checksums and a versioned `file-checksums-v1` fingerprint. An identical committed batch is `SKIPPED_ALREADY_LOADED`, with zero new analytical writes. Reused batch/transformation IDs with different checksums reject.

Legacy fingerprints are not assumed equivalent. A legacy report without provable file checksums stops with `LEGACY_BATCH_EVIDENCE`; neither registry nor data is overwritten. Genuine corrected upstream data gets a new transformation/batch identity.

Processing period and snapshot end cannot move backward. Each retry has a fresh load attempt ID. Failure before commit rolls back every table and registry update. Independent local evidence survives rollback or network loss.

If commit acknowledgement is lost, inspect the registry before replay. Matching committed run proves publication; unresolved state is `COMMIT_UNKNOWN`. `reconcile` reconstructs evidence without another data load. Historical restore is a separate owner procedure, not an ordinary replay.

---

## 4. Pre-Load Validation Checks

| Check / target | Purpose | Passing criterion | Severity |
| --- | --- | --- | --- |
| PRE-01 Connection/TLS | Reach intended target | Connection within 10 seconds; valid TLS direct/session mode | Critical |
| PRE-02 Permissions | Ensure runnable publication | Writable database; TEMP and required table privileges; audit writes succeed | Critical |
| PRE-03 Schema | Prevent incompatible persistence | Registered/live columns, widths/scales, defaults, keys, constraints/indexes match; no unreviewed triggers | Critical |
| PRE-04/05/06/07/13 Package | Enforce approved complete input | APPROVED/released/PASS; 13 artifacts; matching headers, SHA-256, counts, lineage, rules/window | Critical |
| PRE-09 Duplicate | Prevent replay inflation | New identity or checksum-proven no-op; conflicting/unverifiable legacy identity refused | Critical guard |
| PRE-10 Freshness/locks | Prevent stale/racing publication | Advisory lock within 30 seconds; non-stale period/end; writer-lock recheck unchanged | Critical |
| PRE-11 Stageability | Preserve values/relationships | Zero parse, width, scale, range, duplicate, orphan or staging-constraint failures | Critical |
| PRE-08 Capacity | Reserve space before publication | Configured DB budget, measured relations, local backup/work space and supplied physical evidence pass | Critical |
| PRE-12 Headroom evidence | Identify managed-storage uncertainty | Fresh project-bound physical-disk observation and sufficient local headroom percentage | Non-critical; missing/stale evidence holds |

### 4.1 Hosted Capacity

`pg_database_size` measures database usage, not managed-host filesystem free space. Check laptop disk separately for frozen inputs, logs and downloaded dumps. Do not treat a local drive or SHOW data_directory as Supabase capacity.

`config/load.toml` sets a conservative activity DB budget of 450,000,000 bytes and hosted reserve of 16,000,000 bytes. Require current usage plus reserve plus three times staged-and-existing relation bytes to fit the budget. This is a configurable guard, not discovery of a paid plan, a purchase, or a physical-space guarantee.

Optional `SPARTMENT_LOAD_CAPACITY_EVIDENCE` points to JSON containing project_ref, timezone-aware observed_at (within 24 hours), available_disk_bytes and source identifying the operator's real dashboard observation. Insufficient observed space is critical. Missing, wrong-project, invalid or stale evidence produces PRE-12 WARNING and requires review. Do not invent an observation. See [Supabase database/disk-size guidance](https://supabase.com/docs/guides/platform/database-size).

Actual SQL storage errors roll back. Unrelated concurrent usage can still exceed a checked reserve. Resource thresholds and measured evidence are logged; unknown physical headroom is never reported as measured free space.

---

## 5. Post-Load Validation and Reconciliation

Run before commit; confirm durability afterward. No check repairs input.

| Check / target | Purpose | Passing criterion | Severity |
| --- | --- | --- | --- |
| POST-01/02 Each table | Detect lost/extra rows | Success: received = loaded + rejected; loaded = inserted = expected; updated/rejected = 0; total target and new-batch count equal expected | Critical |
| POST-03 Keys | Prevent duplicate identities | Enabled PK/natural UQ constraints satisfied | Critical |
| POST-04 References | Prevent orphans | Required references present; every non-null FK matches; valid nullable exceptions retained | Critical |
| POST-05 Required fields | Preserve completeness | No NULL required analytical field/batch ID; no empty required key | Critical |
| POST-06 Money | Detect changed financial values | Exact incoming decimal SUM = target SUM for every money column; empty totals zero | Critical |
| POST-07 All analytical columns | Detect mapping loss hidden by totals | Staging/target equal multisets in both directions, excluding added batch lineage only | Critical |
| POST-08 Execution | Detect errors/interruptions | No unresolved SQL/constraint errors; deferred FKs checked before commit | Critical |
| POST-09 Lineage | Prevent mixed snapshots | Rows, accepted registry and pointer share new batch | Critical |
| POST-10 Duration | Enforce window | Warn after 20 minutes; abort/rollback at budget exhaustion | Warning / critical deadline |
| POST-11 Commit | Prove availability | Durable registry identifies this committed run | Critical |

### 5.1 Failure Accounting

Upstream quarantine is excluded from received/rejected counts. Count only confirmed Load row rejections; do not invent how many rows caused a SQL batch error. After rollback, committed loaded/inserted/updated counts are zero:

```text
received = loaded + rejected + rolled_back + not_attempted
```

Categories are disjoint; a staged then tentatively inserted row counts once. Partial input failures count only rows actually read. A verified duplicate has zero new writes and NOT_APPLICABLE new-write reconciliation, referencing original success. Ambiguous legacy duplicates are refused, not presented as verified no-ops.

---

## 6. Load Metadata and Log Specification

| Artifact | Location |
| --- | --- |
| Attempt/table evidence | `logs/spartment-load/<run_date>/<run_id>.json` and `<run_id>_<table>.json` |
| Completion | `loaded/_handover/<run_id>.json` |
| Frozen input | `loaded/_work/<run_id>/` |
| Confirmed rejects | `quarantine/load/<run_date>/<run_id>/<table>_rejected.csv` |
| Dump/manifest | `backups/spartment-warehouse/<run_id>.dump` and `.json` |
| Archive/retention audit | `logs/spartment-load/_archive/` and `_retention/` |

| Field / format | Purpose |
| --- | --- |
| run_id, transformation_run_id, parent_run_id | Unique attempt and upstream lineage |
| batch_id, processing_period, batch_window_start/end, reference_date | Exact snapshot context |
| target_database/schema/profile, table_name | Destination; actual Supabase database is postgres |
| load_type FULL, write_logic REPLACE_SNAPSHOT | Persistence method |
| load_started_at/ended_at, load_duration_ms | ISO +08:00 timings and monotonic duration, including table timings |
| status, acceptance_decision, published/published_at | Execution/availability rather than file-existence inference |
| records_expected/received/staged/inserted/updated/loaded/rejected | Input and committed accounting |
| records_rolled_back/not_attempted/replaced | Explain failures and old-row removal separately |
| pre_load_result, post_load_result, checks | PASS/WARNING/FAIL, check IDs, severity and measured evidence |
| money_totals, output_checksums, content_checksums | Reconciliation and reproducibility |
| fingerprint, fingerprint_scheme | Versioned new-adapter identity without reinterpreting legacy hashes |
| error_code/message/sqlstate, failed_table/step | Sanitized diagnosis, never raw row/DSN exceptions |
| load_code_version, target_schema_version, rules_version | Reproducibility |
| attempt_number, retry_of_run_id, original_success_run_id | Retry/no-op traceability |
| resource_settings, capacity, backup_path, previous_batch_id | Capacity/recovery basis |
| warning_review | Reviewer/time/reason, named warnings, exact batch/checksums |
| notification_status | Actual console/artifact reporting, not unconfigured delivery |

`warehouse_meta.attempts.report` contains the attempt and per-table details. Accepted batches retain publication evidence/checksum maps in `batches.report`. Final local/attempt records include post-commit confirmation/timing. Keep existing history/schema_contract unchanged.

Monitoring uses status/timing/checks; troubleshooting uses error codes/steps; auditing and lineage use upstream IDs/checksums/versions. Recovery uses the durable batch registry to resolve uncertain commits. Local JSON is atomic; generated data/credentials are ignored by Git. Run IDs have timestamp seconds and a random suffix; retries never overwrite old attempts.

---

## 7. Log Retention, Rejected Data, and Access

| Evidence | Minimum retention / handling | Access |
| --- | --- | --- |
| Table operational logs | 90 days online; gzip archive; at least 365 days total | Loader writes; maintainer reads; retention process archives |
| Summaries, batch history, completion | 365 days minimum; conservatively retained indefinitely by this version | Maintainer/owner; runtime cannot delete batch history |
| Load rejection evidence | 365 days; no automatic deletion | Maintainer/Super Admin; financial deletion needs owner approval |
| Held/failed frozen inputs | Until resolved, then at least 90 days; conservatively retained | Restricted local account access |
| Successful work copies | Remove after durable completion evidence | Original Stage 2 artifacts follow their own retention |
| Logical backups | At least three daily copies/seven days; major-change copy at least 30 days | Owner restores; no automatic expiry in this version |

Before replacing a current snapshot, download a custom-format logical dump of warehouse and warehouse_meta, verify `pg_restore --list` and SHA-256 manifest. Test restore into a separate empty database, never over the live warehouse. This is not a claim about paid Supabase backup features.

`spartment-load-retain` previews by default, archives with --apply, and requires `--delete-expired-archives --approved-by NAME` for archives older than 365 days. It never automatically prunes registry/current pointer, summaries, quarantine or backups. Deletion is audited. Incident holds in `loaded/_holds/` override expiry; unresolved audit/recovery evidence is protected. Malformed/ambiguous evidence must not be assumed expired.

Reject logs store only table/line/check/reason needed for diagnosis. Do not copy contacts, full tenant payloads, passwords, service keys or credential-bearing URLs. Frozen inputs can contain personal data and require restricted filesystem permissions. Database grants do not silently change Windows security policies.

---

## 8. Load Acceptance, Rollback, and Completion Rules

| Condition | Decision / status | Action / responsibility |
| --- | --- | --- |
| All critical PASS, reconciled, no unresolved warnings, commit confirmed | APPROVED / SUCCESS | Publish all tables/pointer atomically; maintainer monitors |
| Non-critical Load warning | MANUAL REVIEW / HELD | Roll back tentative publication; retain package; maintainer/owner records review within one business day |
| Critical schema/data/identity/capacity/constraint failure | REJECTED / FAILED | Whole-batch rollback; preserve current publication; correct defect |
| Confirmed transient connection/lock/deadlock | FAILED, retry eligible | At most three retries after initial attempt; waits 5, 10, 20 minutes |
| Unknown commit acknowledgement | COMMIT_UNKNOWN | Inspect registry; no blind replay; maintainer resolves |
| Checksum-proven identical committed batch | APPROVED / SKIPPED_ALREADY_LOADED | No analytical writes; reference original success |
| Released upstream warning batch | REJECTED | Return to Stage 2; no Load override |

No count/money difference is accepted as tolerance. Every table must pass; successful Load has zero rejects. Critical failures/upstream approval cannot be waived. A warning review names exact checks and batch/checksums with reviewer/time/reason. Review never keeps a transaction open; rerun repeats validation.

Retries stop if the execution window cannot accommodate the next wait/attempt. Schema, identity, storage shortfall, corruption and data defects are not automatically repaired. An unknown commit follows registry reconciliation before retry eligibility.

Completion reports all 13 counts/checks, status, available schema/database, batch/window/reference date, lineage/checksums, publication time and recovery basis. Held/failed runs state that the prior snapshot remains available. Users inspect existing current_batch and the completion record. No new reporting view is assumed.

For this free activity, notification means console, completion/failure JSON and Airflow task status when enabled. The maintainer reviews results and informs the owner. No email/SMS integration is claimed. Reconstruct a missing local completion file from committed evidence; never reload solely because notification failed.

---

## 9. Common Load Problems and Mitigation

| Problem | Cause | Impact | Handling |
| --- | --- | --- | --- |
| Duplicate totals | Replay appended records | Inflated reports | Full refresh, unique keys and locked checksum registry |
| Legacy hash ambiguity | Different fingerprint scheme | Unprovable duplicate identity | LEGACY_BATCH_EVIDENCE; preserve history and refuse replay |
| Partial publication | Crash/per-table commits | Mixed snapshots | Single transaction; tested mid-load rollback |
| Missing parent | Wrong order/incomplete package | FK failure | Parent-first inserts, reverse deletes, reference closure and deferred checks |
| Value truncation/rounding | Existing bounded types/scales | Values change despite matching counts | Width/scale preflight and full-value/money reconciliation |
| Stale overwrite | Historical replay | Reporting moves backward | Compare period/snapshot_end and writer-lock recheck |
| Overwritten CSVs | Another same-date transform | Wrong package | Frozen SHA-256-verified copies |
| Supabase connection failure | Password/project suffix/TLS/network/paused project | Cannot start/interrupted load | Validated session/direct DSN, sanitized diagnostics, bounded transient retries |
| Transaction pooler | Session continuity lost | Temp/lock assumptions fail | Reject port 6543 |
| Unknown physical capacity | Hosted disk is not laptop disk | False confidence/exhaustion | Budget, real dashboard evidence or review hold; SQL errors roll back |
| Concurrent legacy writer | Different advisory convention | Current batch changes while staging | Fixed writer locks plus current/duplicate recheck |
| Commit acknowledgement lost | Network interruption | Unsafe blind replay | Durable registry reconciliation / COMMIT_UNKNOWN |
| Laptop asleep/offline | Local runtime unavailable | Missed/failed job | Keep awake/online; manual recovery |
| Schema/new trigger drift | Unreviewed change | Incompatible or modified data | Strict catalog/trigger check; reviewed migration, no auto-repair |

---

## 10. Commands, Verification and Activity Completion

### 10.1 Supabase Configuration

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[load]"
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli inspect --owner
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli provision-existing --role spartment_activity_loader
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli inspect
```

Use `.env.example`; put real credentials only in `.env`. Copy destination Session pooler parameters from Supabase Connect. The database password is separate from account login/API keys. `inspect` uses a read-only transaction and zero warehouse writes. `provision-existing` verifies the contract, creates a new restricted login and saves the runtime DSN. It never resets an existing role or recreates tables. Provisioning is one-time: the activity login is already provisioned for this workspace, so use `inspect` on subsequent runs. The default name `spartment_pipeline_loader` already existed; it was preserved and the distinct activity name was chosen. The local-reference `setup` command rejects Supabase.

Set SPARTMENT_PG_BIN when pg_dump/pg_restore are not on PATH; optional SPARTMENT_LOAD_CAPACITY_EVIDENCE supplies the actual dashboard observation. Non-secret budgets/timeouts are in config/load.toml. No paid plan upgrade is requested or performed.

### 10.2 Load and Recovery

```powershell
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli run --handover transformed/_handover/TRANSFORMATION_RUN_ID.json --dry-run
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli run --handover transformed/_handover/TRANSFORMATION_RUN_ID.json
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli run --handover transformed/_handover/TRANSFORMATION_RUN_ID.json --review-file REVIEW.json
.\.venv\Scripts\python.exe -m spartment.stage3_load.cli reconcile --run-id LOAD_RUN_ID
.\.venv\Scripts\python.exe -m spartment.stage3_load.retention
```

Replace uppercase IDs with actual artifacts. Dry run stages then rolls back; it is not publication. Review JSON contains batch_id, exact output_checksums, waived_checks, reviewed_by, reviewed_at and review_reason. It cannot bypass critical checks. CLI exits: 0 success/no-op/dry-run/inspection; 1 failed execution; 2 configuration; 5 warning hold; 6 unknown commit.

### 10.3 Tests and Scheduling

```powershell
.\.venv\Scripts\python.exe tools/test_stage3.py --pg-bin "C:/Program Files/PostgreSQL/18/bin"
.\.venv\Scripts\python.exe -m spartment.stage3_load.daily --check-only
```

Tests start a disposable loopback-only PostgreSQL cluster and remove it afterward; they do not use the live owner DSN. Adapter tests restore the schema-only export and use that server's native catalog baseline for PostgreSQL-version rendering differences. The live PostgreSQL 17 catalog check remains strict. Tests exercise success/no-op, stale/legacy rejection, bounded types, capacity holds, roles, backups, rollback, recovery, retention and scheduler guards. [load-verification.md](load-verification.md) records outcomes and deployment limits.

`tools/demo_load.py` generates labeled synthetic inputs; --publish-demo refuses an existing current publication. Do not replace the populated Supabase warehouse with synthetic data. Operational refresh requires a fresh approved real-source package.

Airflow support is in dags/spartment_daily_load.py and needs a supported Linux/WSL/container runtime. Callback tests do not prove scheduler execution. Windows alternative: `python -m spartment.stage3_load.daily`, then `powershell -File tools/register_daily_task.ps1`. Registration creates a disabled task for review. Enable after valid source/runtime settings and successful manual execution. The interactive task requires an awake, connected, logged-in laptop and the intended Asia/Manila machine timezone.

| Lab section | Required completion evidence |
| --- | --- |
| 1. Source/target | Approved package, destination inspection, measured window/counts |
| 2. Tables | Reviewed existing catalog, types, keys, constraints/indexes |
| 3. Strategy | Replica success/no-op/rollback and preserved history |
| 4. Pre-load | Recorded schema/package/capacity/identity gates |
| 5. Reconciliation | All 13 counts and exact value/money checks |
| 6. Metadata | Attempts, accepted batch and completion linked to upstream IDs |
| 7. Retention/access | Restricted role, retention hold/preview, backup restore |
| 8. Acceptance | Warning review, critical rollback, retry and commit recovery |
| 9. Problems | Relevant diagnosed failures and tested handling |

Passing isolated tests does not imply operational refresh or active scheduling. Missing source credentials, eligible incoming package or capacity evidence remain explicit deployment requirements. Existing Supabase rows stay unchanged until validated publication.

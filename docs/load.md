# Spartment: Operational and Analytical Batch Data Pipeline

## Stage 3: Data Load Documentation

### Objective

Load the validated, transformed dataset handed over by Stage 2 into a local PostgreSQL reporting database. Verify that the published data is complete, accurate, traceable, and ready for room occupancy, tenancy, billing, collection, inquiry, and tenant request analysis.

> **Implementation status: proposed specification.** Stage 3 is not yet implemented. `src/spartment/stage3_load/__init__.py` currently contains a placeholder. Target database objects, loader behavior, checks, logs, scheduling, and retention described below are requirements for implementation, not claims of completed or tested functionality. This document alone does not complete the activity's requirement for documentation consistent with an implemented load process.

This specification follows the nine sections of **Stage 3: Data Load Documentation** in the supplied `lab-5 (1).pdf`. Existing Stage 2 interfaces were checked against `contract.py`, `acceptance.py`, `pipeline.py`, and `writer.py` in `src/spartment/stage2_transform/`. The related specifications are [extraction.md](extraction.md) and [transformation.md](transformation.md). Where older prose conflicts with the current Stage 2 interface, the code defines the input described here.

The pipeline is **batch only**. Streaming, micro-batch, and real-time loading are outside scope. Load consumes Stage 2 output only. It must not query the operational Supabase source, repair business values, or repeat extraction or transformation rules.

---

## 1. Load Source and Target Specification

### 1.1 Load Input

| Input | Location or field | Use |
| --- | --- | --- |
| Transformation handover | `transformed/_handover/<transformation_run_id>.json` | Select the exact batch, decision, paths, checksums, and processing window |
| Transformed datasets | Paths in `transformed_datasets`; normally `transformed/<table>/<run_date>/<table>_<run_date>.csv` | The only analytical records loaded |
| Transformation validation | `transformed/_validation/<transformation_run_id>.json` | Confirm the handover decision and validation evidence |
| Transformation run summary | `logs/spartment-transformation/<run_date>/<transformation_run_id>.json` | Obtain each table's `records_output` and confirm lineage |
| Per-table transformation logs | `logs/spartment-transformation/<run_date>/<transformation_run_id>_<table>.json` | Counts and checksums supporting reconciliation |
| Transformation quarantine references | `quarantine_files` in the handover | Explain upstream exclusions; these files are never load input |

CSV files use UTF-8, a header row, and textual values. Their exact columns come from Stage 2 `OUTPUT_COLUMNS`, types from `OUTPUT_TYPES`, and required values from `REQUIRED_NOT_NULL`. No per-table schema sidecar is produced. Load must validate the contract version it supports rather than guess types from sampled values.

| Batch attribute | Existing Stage 2 field | Load rule |
| --- | --- | --- |
| Transformation run | `run_id` | Required; linked from every load attempt |
| Extraction lineage | `parent_run_id` | Retained in load audit metadata |
| Batch identifier | `batch_id`, currently `<transformation_run_id>-b1` | Preserved; not replaced with the load attempt ID |
| Processing date | `processing_period` | Identifies the snapshot date |
| Batch window | `batch_window_start`, `batch_window_end` | Preserved as supplied; not assumed to be one day of source changes |
| Business reference date | `reference_date` | Preserved; Load does not recalculate aging at the current date |
| Transformation version | `rules_version` | Required and checked against the supported contract |
| Acceptance | `acceptance_decision.decision` | Must equal `APPROVED` |
| Release | `released_for_load` | Must be `true` |
| File integrity | `output_checksums` | SHA-256 of each exact CSV must match |
| Reproducibility | `content_checksums` | Retained for lineage and comparison; distinct from file checksums |

**Approval boundary:** Stage 2 currently permits a `MANUAL REVIEW` batch to have `released_for_load=true` after `--allow-manual-review`. This Load specification follows the lab's stricter requirement: the decision must also be `APPROVED`. A released warning batch remains ineligible. An upstream correction and a new approved transformation run are required; editing a handover JSON or overriding the Load gate is prohibited.

Stage 2 output paths are date-scoped and can be overwritten by another run for the same date. Before loading, copy the selected handover, supporting metadata, and CSVs into a private attempt directory and verify every copied checksum. Read only those verified copies throughout the attempt. If they do not match the selected handover, stop and select a consistent approved run.

### 1.2 Target System

| Item | Proposed specification |
| --- | --- |
| System | Spartment local analytical reporting database |
| Database management system | PostgreSQL; use the existing local installation after verifying that its server is configured and running |
| Database | `spartment_warehouse` |
| Published schema | `warehouse` |
| Operational control schema | `load_control` |
| Temporary staging | Connection-local temporary tables with `ON COMMIT DROP` |
| Purpose | Store the latest accepted analytical snapshot for local SQL queries, reports, and the activity demonstration |
| Hosting and cost | User's laptop; no paid cloud hosting or subscription is required |
| Source separation | Separate local database; no writes to the operational Supabase database |

### 1.3 Load Method and Schedule

| Item | Proposed specification |
| --- | --- |
| Loader | Python 3.11+ with the PostgreSQL driver `psycopg` 3; this is a new Stage 3 dependency, not an existing dependency |
| Transfer | Client-side `COPY FROM STDIN` into typed temporary staging tables, followed by explicit-column `INSERT ... SELECT` into published tables |
| Load type | Full refresh of all 13 analytical tables as one accepted snapshot |
| Transaction boundary | All published table replacements, reconciliation, and successful batch registration in one transaction |
| Schedule | Daily at 03:00 Asia/Manila (`0 3 * * *`), subject to successful upstream completion |
| Orchestration | Proposed Airflow DAG `spartment_daily_load`; manual execution supported for demonstration |
| Dependencies | Exact processing-date extraction and transformation runs must pass their gates; a later clock time alone does not establish readiness |
| Concurrency | One active load; a PostgreSQL advisory lock also protects manual and scheduled attempts from overlapping |
| Local availability | Laptop on and awake, PostgreSQL running, and the execution environment available throughout the run |
| Missed schedule | No guarantee of automatic catchup; operator selects the latest eligible approved batch and starts a manual run |

Airflow itself requires a supported execution environment. On this Windows laptop, scheduling can be implemented in Linux through WSL2 or Docker. The environment must be able to access the repository's persistent data directories and connect to the local PostgreSQL service. Using `localhost` from inside a container does not automatically address the Windows host. Connection routing must be tested during setup. Manual loading does not require a running Airflow scheduler.

No load CLI, DAG, database migration, or retention command exists yet. Their names and flags must be added to this document only after implementation verifies the actual interface.

### 1.4 Scope, Volume, and Completion Window

Load all 13 outputs listed in Section 2; partial-table refresh is prohibited because dependent tables must describe the same snapshot. Expected rows for table `t` are the selected transformation summary's `tables[t].records_output`, verified against its CSV. Do not use Stage 2's source `records_read` as the expected load count: transformation can merge identities, discard duplicates, reject records, and generate date rows.

Actual batch volume has not been measured. The first demonstration must record all per-table counts, total CSV bytes, staging size, target size, and duration. Zero rows are valid when the approved handover and corresponding table log both declare zero; unexplained empty or missing tables fail validation.

The proposed load window is **03:00-03:30 Asia/Manila**. The 30-minute target is an activity design objective, not measured performance. Warn at 20 minutes. Cancel work and roll back if the batch cannot finish within 30 minutes; use a remaining-time budget for each operation rather than allowing every statement 30 minutes. Connection timeout is 10 seconds and lock timeout is 30 seconds. A manual demonstration uses the same 30-minute duration limit from its actual start.

### 1.5 Prerequisites, Roles, and Limitations

| Resource or role | Requirement |
| --- | --- |
| Laptop | Enough available disk space for staged CSV copies, PostgreSQL staging and refresh work, logs, and backup |
| Python | Existing pipeline installation plus the proposed Stage 3 PostgreSQL driver |
| PostgreSQL | Running local server and a database initialized from the eventual warehouse migration |
| Input | One complete, approved Stage 2 handover and all supporting artifacts |
| Database owner | Creates database, schemas, roles, constraints, and indexes; performs migrations and restores |
| `spartment_loader` | `CONNECT`; schema `USAGE`; database `TEMP`; `SELECT`, `INSERT`, and `DELETE` on the 13 warehouse tables; appropriate control-table access; no superuser, source-database, or schema-alteration privilege |
| `spartment_reporter` | `CONNECT`, warehouse `USAGE`, and `SELECT` only; no staging or rejection-file access |
| Data Pipeline Maintainer | Runs and reviews the activity; owns diagnosis, retries, and retention |
| Super Admin / Database Owner | Reviews warnings and approves exceptional restores or deletion of retained financial evidence; the student may perform these roles locally |

Credentials belong in local environment variables or the configured Airflow connection, never in Markdown, version control, command examples, or logs. The existing Supabase extraction credential is not a local PostgreSQL loading credential.

Internet access is needed for upstream Supabase extraction. Loading an already available, approved local Stage 2 package into local PostgreSQL does not itself require internet access. The latest-snapshot target does not preserve previous daily states; backups and audit records provide recovery and traceability, not a queryable history warehouse.

---

## 2. Target Tables and Column Specification

### 2.1 Shared Contract and PostgreSQL Types

Create one `warehouse` table for each Stage 2 output. Preserve **all** columns in `OUTPUT_COLUMNS`, including audit and quality flags. The table below specifies the common mapping; Section 2.2 lists the important columns rather than reproducing data rows.

| Stage 2 family | Proposed PostgreSQL type | Storage rule |
| --- | --- | --- |
| `surrogate` | `varchar(16)` | Preserve existing 16-character hexadecimal keys from Stage 2; never regenerate them |
| UUID | `uuid` | Preserve the UUID value |
| Integer | `integer` | Reject overflow; no silent truncation |
| Money | `numeric(18,2)` | Exact decimals; reject nonzero precision loss or overflow rather than round again |
| Rate | Unconstrained `numeric` | Preserve supplied precision; no new rounding |
| Area | Unconstrained `numeric` | Preserve supplied precision and existing out-of-bounds flags |
| Boolean | `boolean` | Parse Stage 2's `true` / `false` representation strictly |
| Date | `date` | Parse the supplied `YYYY-MM-DD` |
| Timestamp | `timestamptz` | Preserve the instant and use Asia/Manila for session display |
| Code or text | `text` | Preserve spelling, case, whitespace, phone formatting, and identifiers |
| `dim_date.date_key` | `varchar(8)` | Preserve the supplied `YYYYMMDD` string |

Stage 2 defaults unlisted fields to text. In particular, `previous_values`, `corrected_values`, and `details` are carried as **text** in this target to preserve the supplied representation, including diagnostic content. Do not silently convert them to `jsonb` or enforce JSON validity when the approved upstream contract permits the accompanying `json_valid` flag.

Match each table's `NOT NULL` columns exactly to Stage 2 `REQUIRED_NOT_NULL`. All other incoming columns remain nullable. `COPY` must use an explicit column list and type-aware null handling: an empty optional text value stays an empty string, while an empty nullable typed value becomes SQL `NULL`. Stage 2's CSV serialization does not distinguish a missing text value from an empty text value; Load cannot reconstruct that distinction and must document it rather than invent information. Required typed empties fail. Do not substitute defaults for missing analytical values.

Every published row additionally receives `load_run_id text NOT NULL`, `load_batch_id text NOT NULL`, and `loaded_at timestamptz NOT NULL`. These are Load lineage fields supplied explicitly by the loader. Preserve existing `transformation_run_id`; `dim_date` has no such incoming column, so its transformation lineage is resolved through `load_batch_id` and the batch registry. There are no SCD Type 2 history columns or analytical-column defaults.

### 2.2 Table Specification and Inclusion

All key references below are surrogate keys unless identified as natural keys. PK means primary key; UQ means unique constraint. Every listed natural key is `NOT NULL` and unique in the published snapshot.

| Order / target table | Type and reason for inclusion | Primary / natural keys | Important columns and types | Foreign keys |
| --- | --- | --- | --- | --- |
| 1. `dim_date` | Dimension; calendar and monthly grouping | PK `date_key`; UQ `date` | `date date`; `year`, `quarter`, `month`, `week`, `day_of_month`, `day_of_week integer`; `billing_period text`; `is_weekend boolean` | None |
| 2. `dim_building` | Dimension; building identity and occupancy summary | PK `building_key`; UQ `building_id uuid` | `name`, `address text`; room counts `integer`; `occupancy_rate numeric`; `vacancy_gap numeric(18,2)`; `created_at timestamptz` | None |
| 3. `dim_room` | Dimension; availability, location, capacity, and rental rate | PK `room_key`; UQ `room_id uuid` | `building_id uuid`; `room_number`, `capacity`, `status`, `status_raw text`; `floor integer`; `size_sqm numeric`; `monthly_rate numeric(18,2)`; availability and quality flags `boolean` | `building_key -> dim_building.building_key` |
| 4. `dim_tenant` | Dimension; unified tenant/staff identity and current assignment attributes | PK `tenant_key`; UQ `tenant_id uuid` | `full_name`, `email`, `phone`, `role`, `identity_source text`; `current_assignment_id uuid`; `monthly_rent numeric(18,2)`; `move_in`, `lease_end date`; `outstanding_bill_count integer`; active/staff/tenant flags `boolean` | None; `current_assignment_id` is retained without a reverse FK to `fact_assignment` |
| 5. `dim_lease` | Dimension; tenancy relationship used by billing and occupancy events | PK `lease_key`; UQ `lease_id uuid` | `tenant_id`, `room_id uuid`; `move_in_date`, `lease_end_date date`; `status text`; `lease_days integer`; lifecycle flags `boolean` | `tenant_key -> dim_tenant`; `room_key -> dim_room` |
| 6. `fact_assignment` | Fact; one accepted assignment and its tenancy attributes | PK `assignment_key`; UQ `assignment_id uuid` | `monthly_rent numeric(18,2)`; `move_in`, `lease_end date`; `status text`; `tenure_days integer`; `is_active boolean` | `tenant_key -> dim_tenant`; nullable `room_key -> dim_room` |
| 7. `fact_billing` | Fact; one bill, its components, payment rollup, and aging | PK `bill_key`; UQ `bill_id uuid` | Components, `total_amount`, `amount_paid`, `balance numeric(18,2)`; `billing_period_date`, `due_date date`; `billing_period`, `status`, `overdue_bucket text`; aging fields `integer`; `collection_rate numeric` | `lease_key -> dim_lease`; `tenant_key -> dim_tenant`; `room_key -> dim_room` |
| 8. `fact_payment` | Fact; one cash receipt, including correction indicators | PK `payment_key`; UQ `payment_id uuid` | `amount numeric(18,2)`; `payment_date date`; payment rank and correction count `integer`; `reference_note text`; `is_corrected boolean` | `bill_key -> fact_billing`; `lease_key -> dim_lease`; `tenant_key -> dim_tenant` |
| 9. `fact_payment_correction` | Fact; one payment correction audit event | PK `correction_key`; UQ `correction_id uuid` | `payment_id`, nullable `corrected_by uuid`; `reason`, `previous_values`, `corrected_values text`; `json_valid boolean`; `created_at timestamptz` | `payment_key -> fact_payment` |
| 10. `fact_inquiry` | Fact; one prospective tenant inquiry | PK `inquiry_key`; UQ `inquiry_id uuid` | `full_name`, `email`, `phone`, `preferred_room`, `status text`; `move_in_date date`; `age_days integer`; `is_open boolean`; `created_at timestamptz` | Nullable `room_key -> dim_room` |
| 11. `fact_tenant_request` | Fact; one maintenance or other request | PK `request_key`; UQ `request_id uuid` | `kind`, `subject`, `priority`, `status text`; `created_at`, `updated_at timestamptz`; nullable `resolution_days integer`; `is_open boolean` | `tenant_key -> dim_tenant`; nullable `room_key -> dim_room` |
| 12. `fact_occupancy_event` | Fact; one move-out or transfer event | PK `event_key`; UQ `event_id uuid` | `event_type`, `details text`; `json_valid boolean`; `created_at timestamptz` | `tenant_key`, `performed_by_tenant_key -> dim_tenant`; `previous_lease_key -> dim_lease`; nullable `new_lease_key -> dim_lease` |
| 13. `fact_notification` | Fact; one notification audit record | PK `notification_key`; UQ `notification_id uuid` | `kind`, `title text`; nullable `source_id uuid`; `is_read boolean`; `created_at timestamptz` | `tenant_key -> dim_tenant` |

No FK is added from `corrected_by` to inaccessible `auth.users`. `fact_notification.source_id` is polymorphic and has no single target FK. `dim_date` supports date-based joins using existing date columns; do not invent incoming date-key columns in fact tables.

### 2.3 Constraints, Indexes, and Partitioning

| Constraint or index | Specification |
| --- | --- |
| PK and UQ | Apply Section 2.2 keys; PostgreSQL creates their supporting unique indexes |
| Required values | Mirror `REQUIRED_NOT_NULL`; Load lineage columns are also required |
| Referential integrity | Enforce Section 2.2 relationships with `NO ACTION`; nullable references permit SQL `NULL`, never an arbitrary missing non-null key |
| Domains | Use the supported Stage 2 `CODE_DOMAINS`, including `UNKNOWN` where allowed; no new status normalization |
| Non-negative money | Mirror Stage 2 `NON_NEGATIVE`: room monthly rate, building vacancy gap, assignment rent, billing components/total/paid/balance, and payment amount |
| Key format | Surrogate keys contain exactly 16 hexadecimal characters; date key contains eight digits |
| Calendar checks | Quarter 1-4, month 1-12, ISO weekday 1-7, week 1-53; verify consistency against supplied date without modifying it |
| Size and rate limits | Preserve upstream flags; do not add a rejecting room-size range constraint or assume a rate must be at most 1 when Stage 2 permits it |
| FK indexes | Add indexes on referencing warehouse FK columns to support joins and integrity checks |
| Reporting indexes | `fact_billing(tenant_key, status, due_date)` and `fact_payment(bill_key, payment_date)`; assess others after measuring local query needs |
| Batch index | Index `load_batch_id` on each target table for lineage checks |
| Partitions | None for the initial activity; no measured volume justifies partition maintenance |

### 2.4 Control Tables

| Proposed object | Relevant columns and purpose |
| --- | --- |
| `load_control.load_attempt` | PK `run_id text`; `batch_id`, `transformation_run_id`, `parent_run_id text`; processing/reference dates; batch window fields; timestamps; status, decision, code version; pre/post-check evidence and errors; one row per attempt, including failures |
| `load_control.load_table_result` | PK `(run_id, table_name)`; received/staged/inserted/updated/rejected/rolled-back/not-attempted counts; expected count, monetary totals, durations, and validation result |
| `load_control.loaded_batch` | PK `batch_id`; UQ `transformation_run_id`; committed load run, processing/reference dates, artifact checksums, row counts, and completion timestamp; inserted atomically with publication |
| `load_control.current_snapshot` | One singleton row identifying the published batch and its run, processing date, reference date, and publication timestamp; updated atomically with warehouse data |

Control-table statuses and decisions use explicit CHECK domains. They reference attempts by `run_id`; failed-attempt evidence must survive rollback through the separate audit channel described in Section 3.4. The loader may write attempt/results, insert a committed batch, and update the singleton; deleting historical registry records is restricted to the authorized retention process.

---

## 3. Load Strategy and Rules

### 3.1 Per-Table Load Rules

| Tables | Load type / write logic | Identity comparison | Changing data / history | On failure |
| --- | --- | --- | --- | --- |
| All five dimensions | FULL; replace snapshot rows from verified staging | Existing PK and natural key in Section 2.2; used for validation, not an incremental merge | Current attributes overwritten, equivalent to SCD Type 1; no Type 2 history | Roll back whole batch |
| All eight facts | FULL; replace snapshot rows from verified staging | Existing fact PK and natural key in Section 2.2 | Preserve supplied events and current corrected values; no append-only assumption | Roll back whole batch |

For **both** rows above, failure rolls back the entire publication transaction and stops the batch. No table may be published independently. Full refresh is selected because the upstream extraction is full and reliable change tracking is not established. Incremental watermarks, history-key generation, and SCD Type 2 are not part of this activity design.

Rules:

| Rule ID | Condition and action |
| --- | --- |
| L-01 | Accept only a complete, checksum-verified, approved Stage 2 package with `released_for_load=true` |
| L-02 | Load all 13 tables; no subsets and no reads from operational Supabase or raw extraction CSVs |
| L-03 | Preserve surrogate/natural keys and analytical values; perform only lossless storage parsing and Load lineage assignment |
| L-04 | Reject duplicate incoming keys and non-null orphan references; never fabricate an Unknown member |
| L-05 | Preserve legitimate nulls, including ended assignments without rooms, inquiries without rooms, other requests without rooms, and move-out events without new leases |
| L-06 | Preserve inactive records present in the package. A record absent from the new full snapshot is absent from the new published snapshot; do not infer a source deletion or invent a soft-delete flag |
| L-07 | Late arrivals enter through a later approved full Stage 2 snapshot. Upstream stages own repairs and recalculation; Load does not fetch missing records |
| L-08 | Contract mismatch stops the batch before publication; no automatic target alteration or dropped extra columns |
| L-09 | An already committed batch with identical checksums is a recorded `SKIPPED_ALREADY_LOADED` no-op. The same batch ID with changed checksums is rejected as an identity conflict |
| L-10 | Reject a package older than the current published processing date or reference date. A newer transformation for the same date may replace it after full validation; historical restore requires a separate documented owner procedure |
| L-11 | Acquire one warehouse-wide advisory lock before checking duplicate/current batch state and keep it until publication completes; no concurrent writers |

### 3.2 Staging and Load Order

1. Create a unique load attempt, record `RUNNING`, and acquire the advisory lock.
2. Verify and freeze the input package; execute Section 4 checks.
3. Begin the publication transaction. Create typed temporary staging tables and bulk-copy all supplied rows, using explicit input columns.
4. Check staging counts, keys, values, reference closure, and financial totals before changing the published data.
5. Lock the 13 published tables in a fixed order that blocks writers while allowing ordinary readers. Remove existing rows in **reverse** Section 2.2 order, then insert staged rows in Section 2.2 order, assigning Load lineage fields.
6. Run Section 5 checks against the replacement data inside the same transaction.
7. If all critical checks pass and there are no unresolved warnings, insert the committed batch record, update `current_snapshot`, record success, and commit once.
8. Write the completion artifact and display the result. Release the lock and remove temporary working copies after audit evidence is durable.

Use transactional `DELETE` and `INSERT` for this local design; do not use `TRUNCATE ... CASCADE`, disable constraints, drop tables, or rename away objects used by reporting queries. PostgreSQL keeps uncommitted replacements invisible to other sessions. A report containing several separate statements should use a read-only repeatable-read transaction if it must see the same snapshot throughout the report.

### 3.3 Idempotency and Recovery

Duplicate checks use the durable batch registry under the advisory lock. Repeating a successful batch creates no analytical writes, reports zero inserts/updates, references the original success, and leaves `current_snapshot` unchanged. It does not republish an old batch that has since been superseded.

A failed batch can be retried with a new `run_id`, the same batch identity, and the same verified files. Rollback ensures that publication restarts from the previous accepted snapshot. A corrected upstream package must use a new transformation run and batch ID; do not reuse an ID with changed files.

If a connection breaks while commit is being acknowledged, the result is **unknown**, not automatically failed. Reconnect and inspect `loaded_batch` and `current_snapshot`. A matching committed registry entry proves publication; its absence permits retry only after the original transaction has ended. If database state cannot be established, record `COMMIT_UNKNOWN` locally, hold retries, and require maintainer reconciliation.

### 3.4 Rollback and Failure Evidence

Any critical check, SQL error, cancellation, or interruption before commit triggers whole-batch rollback. No changed warehouse rows or successful batch registration remain. Temporary staging tables disappear when their transaction/session ends.

Write the initial attempt record and append-only local diagnostic events outside the publication transaction. After rollback, use a separate short audit transaction to record the final failure and table results. If PostgreSQL is unavailable, preserve local JSON evidence and reconcile it on recovery. Never log `SUCCESS` merely because `COPY` completed.

When warnings require manual review, roll back any tentative replacement and retain the verified attempt package. Review must not hold a database transaction open. A reviewer records a decision; an authorized rerun repeats all checks against the unchanged package and any documented waiver of a **non-critical Load warning**. Critical failures and upstream `MANUAL REVIEW` cannot be waived.

---

## 4. Pre-Load Validation Checks

Each critical failure stops the load. A non-critical warning holds publication for review. Thresholds below are proposed activity settings and must be configurable and logged when implemented.

| ID / check name | Target | Purpose | Measurable criteria | Severity |
| --- | --- | --- | --- | --- |
| PRE-01 Connection | Local PostgreSQL | Establish target access | Connect and execute a readiness query within 10 seconds | Critical |
| PRE-02 Write permissions | Loader role and target objects | Prevent mid-run authorization failures | Required schema/table privileges and database TEMP privilege all present; read-only transaction setting is off | Critical |
| PRE-03 Schema match | All 13 warehouse tables and control objects | Prevent incompatible storage | All expected columns, types, nullability, keys, FKs, lineage fields, and supported schema version match; no unsupported required target extras | Critical |
| PRE-04 Approved handover | Handover and validation JSON | Load approved data only | Decision exactly `APPROVED`, release true, linked run IDs and validation evidence agree, no critical FAIL | Critical |
| PRE-05 Package completeness | Dataset map and supporting logs | Avoid a partial snapshot | Exactly the required 13 datasets accounted for; required metadata and validation files exist, parse, and identify the selected run | Critical |
| PRE-06 File integrity | Frozen CSV copies | Detect overwritten/corrupt files | All 13 SHA-256 hashes equal `output_checksums`; all headers match `OUTPUT_COLUMNS`; no truncated rows, duplicate headers, or malformed CSV | Critical |
| PRE-07 Record counts | CSVs and per-table results | Establish the expected batch size | Parsed row count equals `records_output` for each table; identifiers agree across summary and table logs | Critical |
| PRE-08 Storage readiness | CSV-copy, PostgreSQL data/WAL/temp, backup, and log volumes | Reserve capacity for safe refresh | Available bytes on every involved volume satisfy Section 4.1; missing evidence fails closed | Critical |
| PRE-09 Duplicate batch | `loaded_batch` | Prevent duplicate application | Unseen batch proceeds; identical committed batch exits as no-op; same ID with different hashes rejects | Critical guard |
| PRE-10 Concurrency and freshness | Advisory lock and `current_snapshot` | Avoid races and stale publication | Lock obtained within 30 seconds; incoming processing/reference dates are not older than current values; duplicate check repeated under lock | Critical |
| PRE-11 Stageable values | Temporary staging and input parser | Detect storage-incompatible values | All rows parse losslessly; required values present; zero duplicate PK/natural keys, disallowed domains, overflows, or FK mismatches | Critical |
| PRE-12 Resource pressure | Free disk after reservation | Detect reduced operating headroom | Warn when remaining space is below 20% of volume capacity although PRE-08 still passes | Non-critical |
| PRE-13 Version and lineage | `rules_version`, run IDs, row `transformation_run_id` | Keep the batch reproducible | Supported transformation contract; row IDs match selected run for all tables carrying the field; `dim_date` linked through control metadata | Critical |

### 4.1 Storage Measurement

Before the first target load, make a staging-only capacity probe with the verified batch, measure staged relation sizes, and roll it back. For later loads, use the greater of current measurements and the last measured comparable batch. Record the measurement and estimate basis.

Reserve at least **1 GiB plus three times the measured incoming staged relation size** on the PostgreSQL volume for staging, inserted tuples/indexes, WAL, and maintenance overhead. Separately reserve the verified CSV-copy bytes on the working volume, the projected dump size on the backup volume when a backup is due, and 100 MiB for logs. Combine requirements when volumes are shared. These are conservative activity thresholds, not a universal PostgreSQL sizing formula. Recheck free space during a large copy; actual disk exhaustion always fails and rolls back. If a probe cannot run safely or disk capacity cannot be measured, stop and report the missing resource evidence.

For this local setup, the maintainer or local runner can obtain filesystem capacity without granting the loader PostgreSQL superuser access. Storage-monitoring rights and database-data directory location must be established during setup.

---

## 5. Post-Load Validation and Reconciliation

Run these checks on the replacement warehouse rows **before commit**, then verify the committed registry and current pointer afterward. No cleaning or recalculation is performed when a check fails.

| ID / check name | Target | Purpose | Passing criteria | Severity |
| --- | --- | --- | --- | --- |
| POST-01 Record accounting | Each table | Account for all rows received | On a successful batch, `received = loaded + rejected`, loaded = inserted = expected, updated = 0, rejected = 0 | Critical |
| POST-02 Target count | Each table filtered by incoming `load_batch_id` | Detect missing/extra rows | Target count equals verified CSV count; total table count equals that batch count for full refresh | Critical |
| POST-03 Unique keys | Section 2 PK/natural keys | Prevent duplicate identities | Zero null or duplicate keys; constraints enabled and valid | Critical |
| POST-04 Referential integrity | Every Section 2 FK | Prevent orphan facts/dimensions | Every non-null FK matches its parent; required FKs not null; legitimate nullable references retained | Critical |
| POST-05 Required values | Contract and Load lineage columns | Preserve completeness | Zero SQL NULL in required columns; required key text not empty | Critical |
| POST-06 Money reconciliation | Billing components/total/paid/balance; payment amount; assignment rent; room monthly rate; building vacancy gap | Detect lost or altered financial values | Each per-table incoming decimal sum equals the target sum exactly; empty-set totals treated as zero | Critical |
| POST-07 Value preservation | All incoming columns of all tables | Detect incorrect mapping or value changes hidden by totals | Typed staging and target projections equal as multisets in both directions, excluding only added Load lineage; no differences | Critical |
| POST-08 Errors and interruptions | Loader/SQL diagnostics | Confirm complete execution | Zero unhandled COPY/INSERT errors, constraint failures, cancellations, or unfinished table steps | Critical |
| POST-09 Snapshot lineage | All tables and batch registry | Prevent mixed batches | Every row has the new batch and load run; incoming transformation lineage preserved; registry and pointer identify that batch | Critical |
| POST-10 Duration | Whole attempt | Monitor completion window | Warning above 20 minutes; abort/rollback before exceeding the 30-minute attempt budget | Non-critical warning; critical deadline |
| POST-11 Commit confirmation | Committed registry and pointer | Prove publication | Durable registry entry matches checksums and counts; pointer identifies accepted publication, or a later accepted publication if verification occurs afterward | Critical; unknown state follows Section 3.3 |

Money is already standardized by Stage 2. Use exact decimal arithmetic and PostgreSQL numeric aggregates, without floating point or additional rounding. Exact equality is appropriate because Load copies the same standardized values; it is not a new source-to-business reconciliation with a rounding tolerance.

### 5.1 Accounting for Failure, Rejection, and No-Op Runs

Transformation-rejected records are outside `records_received`. Only rows in the approved transformed files are received by Load. Upstream quarantine counts may be referenced separately for lineage but must not be added to load rejections.

`records_rejected` counts individually identified incoming rows that cannot be written, such as invalid typed values or constraint violations. A transactional SQL failure may expose only one offending row before aborting; report only confirmed row rejects and record a batch-level failure for the rest. Never fabricate rejection counts.

For a failed attempt, `records_loaded` is the number committed by that attempt and is zero after rollback. Record the separate accounting identity:

```text
records_received = records_loaded + records_rejected
                   + records_rolled_back + records_not_attempted
```

The categories are mutually exclusive. `records_rolled_back` includes accepted rows already staged or tentatively inserted that were discarded; `records_not_attempted` includes the remaining rows not successfully processed. Do not double-count a row copied into staging and then inserted. The success-only two-term equation is not reported as passing on a failed or skipped attempt.

For an identical committed duplicate, status is `SKIPPED_ALREADY_LOADED`, analytical writes and rejections are zero, and reconciliation is `NOT_APPLICABLE` for new writes. Reference the original successful batch's counts and evidence.

---

## 6. Load Metadata and Log Specification

### 6.1 Locations and Naming

| Artifact | Proposed location |
| --- | --- |
| Table result JSON | `logs/spartment-load/<run_date>/<run_id>_<table>.json` |
| Attempt summary and validation evidence | `logs/spartment-load/<run_date>/<run_id>.json` |
| Completion record | `loaded/_handover/<run_id>.json` |
| Frozen attempt package | `loaded/_work/<run_id>/` |
| Load rejection evidence | `quarantine/load/<run_date>/<run_id>/<table>_rejected.csv` |
| Archived operational logs | `logs/spartment-load/_archive/<run_date>/` |
| Retention audit | `logs/spartment-load/_retention/` |
| Target backup | `backups/spartment-warehouse/<timestamp>.dump` plus manifest |

The proposed run ID is `spartment-load-YYYY-MM-DD-HHMM`, with a counter suffix when needed. Every retry gets a new run ID linked to the original attempt. Use atomic writes for JSON files. Generated files must be excluded from version control when implementation introduces these directories.

### 6.2 Field Specification

The values below illustrate formats; they are not results from an executed load.

| Field | Purpose | Example / format |
| --- | --- | --- |
| `run_id` | Unique load attempt | `spartment-load-2026-11-12-0300` |
| `transformation_run_id` | Immediate upstream lineage | `spartment-transform-2026-11-12-0200` |
| `parent_run_id` | Extraction lineage | `spartment-extract-2026-11-12-0100` |
| `batch_id` | Unchanged Stage 2 batch identity | `spartment-transform-2026-11-12-0200-b1` |
| `processing_period` | Snapshot processing date | `2026-11-12` |
| `batch_window_start`, `batch_window_end` | Declared upstream boundaries | Exact values from handover |
| `reference_date` | Date used for upstream derived measures | `2026-11-12` |
| `target_database`, `target_schema`, `table_name` | Destination identification | `spartment_warehouse`, `warehouse`, `fact_billing` |
| `load_type`, `write_logic` | Explain how rows were persisted | `FULL`, `REPLACE_SNAPSHOT` |
| `load_started_at`, `load_ended_at` | Attempt or table timings | ISO 8601 with `+08:00` |
| `load_duration_ms` | Elapsed execution time | Non-negative integer measured with a monotonic clock |
| `status` | Execution state | `RUNNING`, `SUCCESS`, `HELD`, `FAILED`, `COMMIT_UNKNOWN`, `SKIPPED_ALREADY_LOADED`, or `INTERRUPTED` |
| `acceptance_decision` | Publication decision | `APPROVED`, `MANUAL REVIEW`, `REJECTED`; null while unresolved |
| `records_expected`, `records_received` | Incoming count reconciliation | Per-table integers from summary and verified CSV |
| `records_staged` | Successful COPY progress | Diagnostic integer; not a committed row count |
| `records_inserted`, `records_updated`, `records_loaded` | Committed writes | Inserted = loaded; updated = 0 in this full-refresh design |
| `records_rejected` | Confirmed row-level write rejections | Integer with reason-linked evidence |
| `records_rolled_back`, `records_not_attempted` | Explain failed attempts | Integers following Section 5.1 |
| `records_replaced` | Previous snapshot rows removed on commit | Separate count; not an incoming rejection or update |
| `pre_load_result`, `post_load_result` | Validation summary | `PASS`, `WARNING`, `FAIL`, or `NOT_RUN`; no-op reconciliation may be `NOT_APPLICABLE` |
| `checks` | Detailed measurable evidence | Check ID, target, severity, expected, observed, result |
| `source_totals`, `target_totals` | Financial reconciliation evidence | Decimal strings per table/column |
| `output_checksums`, `content_checksums` | Identify selected upstream artifacts | Exact maps from verified handover |
| `error_code`, `error_message`, `sqlstate` | Diagnose failure | Stable loader error, sanitized message, PostgreSQL SQLSTATE when available |
| `failed_table`, `failed_step` | Locate the failure | Table and stage name, or null |
| `load_code_version`, `target_schema_version`, `rules_version` | Reproduce behavior | Actual installed version/commit and supported schema/rules versions |
| `attempt_number`, `retry_of_run_id` | Trace bounded retries | Integer and prior load run ID |
| `original_success_run_id` | Explain duplicate no-op | Existing committed load run ID |
| `reviewed_by`, `reviewed_at`, `review_reason` | Record manual decision | Role/user identifier, timestamp, and non-critical warning disposition |
| `backup_path`, `previous_batch_id` | Support recovery | Applicable dump manifest and prior pointer |
| `published`, `published_at` | Confirm availability | Boolean and confirmed commit time |
| `notification_status` | Track completion/failure reporting | `DISPLAYED`, `PENDING`, or `FAILED`; never a false claim of message delivery |

### 6.3 Monitoring and Recovery Use

The maintainer uses status, timings, and check results to monitor daily runs; error codes and table steps identify failures. Linked run IDs trace a reported value back to transformation and extraction. Checksums and code versions establish which package and rules were used. The batch registry determines whether an interrupted attempt committed and whether replay is safe.

The completion record reports final decision, counts for all 13 tables, validation summary, current published batch, reporting database/schema, and availability timestamp. A failed or held record explicitly states that the prior snapshot remains published. If publication committed but writing the local completion file failed, reconstruct that file from committed control records; do not load the batch again.

---

## 7. Log Retention, Rejected Data, and Access

### 7.1 Retention and Access Policy

These are Stage 3 policies. The existing extraction retention configuration does not yet implement them.

| Material | Minimum retention / location | Access and handling |
| --- | --- | --- |
| Per-table operational logs | 90 days online under `logs/spartment-load/`; then gzip archive until 365 days total | Maintainer and Super Admin read; loader writes; designated retention process archives |
| Attempt summaries, check evidence, completion records, and batch registry | 365 days | Preserve lineage and decisions; no routine manual modification |
| Load rejection records | 365 days under `quarantine/load/` | Maintainer/Super Admin only; loader writes; deletion of financial rejection evidence requires Super Admin approval |
| Frozen successful package | Until completion evidence and any required backup are confirmed; original Stage 2 artifacts follow their own retention policy | Work copies may then be removed; keep checksums and lineage in audit evidence |
| Frozen held/failed package | Until resolved, then at least 90 days from final disposition | Keep longer if required for investigation, auditing, or recovery |
| Pre-load backups | Keep at least the last three daily pre-refresh backups and for at least seven days; major-load backup at least 30 days | Owner restores; loader cannot delete backups; unresolved investigations extend retention |

Backups are taken before replacing an existing snapshot, and before the first refresh following a schema/loader change, an exceptional reload, or an approved volume increase above twice the previous incoming row count. The initial empty database needs no prior-data dump. Use a PostgreSQL custom-format logical dump and retain its timestamp, schema version, last published batch, and checksum in a manifest. Verify dump readability, and demonstrate a restore into a separate test database before claiming backup recovery has been validated. Do not overwrite the working target during a restore test.

Minimize rejection payloads. Store run/batch/table, source key or row number, check ID, SQLSTATE, and reason. Store only diagnostic values needed for the failure, mask phone numbers, and avoid copying full names, email addresses, emergency contacts, free-text messages, or full records by default. The frozen input package contains personal information and receives the same restricted filesystem access. Never retain database passwords, service-role keys, or connection strings containing secrets.

### 7.2 Archiving and Deletion

A future retention command must default to a dry run, enforce the documented age checks, and write an audit record for every archive/deletion. No record may be deleted before its minimum period or while linked to an open review, incident, audit, unknown commit, or recovery operation. Apply a documented hold that overrides age-based expiry.

The maintainer authorizes expiry; the retention process performs it. Do not delete the current snapshot's registry entry, attempts/results it references, or lineage needed by retained evidence. Database FK dependencies govern deletion order. Deletion audit records are retained for at least 365 days. Archiving is not deletion, and a local backup on the same laptop does not protect against losing that laptop.

---

## 8. Load Acceptance, Rollback, and Completion Rules

### 8.1 Decision Rules

| Condition | Decision / status | Pipeline action | Decision mode and responsible role |
| --- | --- | --- | --- |
| All critical checks pass, no warnings, exact count/value reconciliation, commit confirmed | `APPROVED` / `SUCCESS` | Commit all tables and registry together; publish completion record | Automatic; maintainer monitors |
| Non-critical Load warning with critical checks passing | `MANUAL REVIEW` / `HELD` | Roll back tentative publication, retain package, display review requirement; prior snapshot remains available | Maintainer and Super Admin review within one business day; approved rerun repeats checks with recorded warning disposition |
| Critical check failure, unsupported schema, invalid values, or constraint violation | `REJECTED` / `FAILED` | Roll back; preserve diagnostic evidence; stop until defect is resolved | Automatic rejection; maintainer investigates and Database Owner handles target defects |
| Transient connection loss before commit, lock timeout, or deadlock with rollback confirmed | Failed attempt; retry eligible | Roll back, log attempt, retry within limits below | Automatic bounded retry; maintainer notified after exhaustion |
| Commit result uncertain | Decision unresolved / `COMMIT_UNKNOWN` | Verify registry before any replay; hold if unresolved | Maintainer and Database Owner reconcile |
| Identical batch already committed | `APPROVED` / `SKIPPED_ALREADY_LOADED` | No analytical writes; report original publication and current snapshot | Automatic |
| Upstream decision is `MANUAL REVIEW` or `REJECTED`, even if files exist | `REJECTED` / `FAILED` | Refuse before warehouse writes; return to Stage 2 owner | Automatic; no Load override |

No row-count difference is treated as a harmless tolerance. This full-refresh activity requires zero load rejections for successful publication. A count mismatch, non-null orphan, missing required value, or monetary difference is critical.

### 8.2 Retries and Failure Notification

Allow **three retries after the initial attempt** for confirmed transient failures only, with waits of 5, 10, and 20 minutes. Each attempt has a new load run ID and retains the original batch linkage. Do not start a retry that exceeds the scheduled 03:30 deadline; stop and require a manual run with a new execution window. The limit is not a promise that four attempts fit in the scheduled window.

Schema changes, upstream approval failures, corrupt files, storage shortfalls, and data/constraint defects require correction before manual rerun; automatic retries do not repair them. Unknown commit results follow Section 3.3 before retry eligibility is considered.

For this no-cost local activity, notification consists of the console result, the completion/failure JSON record, and the Airflow task status when scheduling is used. The Data Pipeline Maintainer checks these after each demonstration or scheduled run and informs the Database Owner for target failures. No email, SMS, or messaging integration is assumed or claimed. Failure to send an optional future external notification must not cause a second data load after commit.

### 8.3 Completion and Downstream Handover

The batch becomes available only after a confirmed commit. Downstream users inspect `load_control.current_snapshot` and the completion record to identify the current batch, publication time, and PASS results. The demonstrator displays these alongside query results from the reporting database. Reports must not read temporary staging tables or use file existence as proof of publication.

| Required completion information | Contents |
| --- | --- |
| Final status | Decision, published flag, committed run, and timestamp |
| Validation summary | Pre-load and post-load result for every analytical table and overall result |
| Record reconciliation | Expected, received, inserted, updated, loaded, rejected, and any failure-accounting categories |
| Data location | Local database `spartment_warehouse`, schema `warehouse` |
| Lineage | Transformation/extraction IDs, batch/window/reference date, checksums, and code/schema versions |
| Recovery information | Previous batch, relevant backup, failure step, retry/review disposition |

**Illustrative summary format, not an executed result:**

| Validation summary | Result |
| --- | --- |
| Five dimension loads | PASS for each table |
| Eight fact loads | PASS for each table |
| Count and money reconciliation | PASS |
| Commit confirmation | PASS |
| Overall Load | APPROVED |

The implemented summary must expand this into 13 named table results and actual measured counts. Do not replace this example with invented results.

### 8.4 Local Operations Runbook

1. Start the laptop and PostgreSQL. For scheduled execution, also start the supported Airflow environment and keep the laptop awake. Verify local database routing from that environment.
2. Finish extraction and transformation for the intended processing date. Select the exact approved transformation handover, not simply the newest CSV directory.
3. Run Load's future preflight/dry-run interface, inspect counts, approval, checksums, schema readiness, and storage evidence. Exact commands will be documented after implementation.
4. Start the full load. Observe its final decision and confirm `current_snapshot` and all 13 table results.
5. For a held or failed attempt, inspect the summary, preserve evidence, fix the appropriate upstream/target defect, and rerun only under the Section 3 recovery rules.
6. Demonstrate report queries and the original batch lineage. The laptop can be turned off after all processes finish; local reports and scheduled jobs are unavailable while it is off.

---

## 9. Common Load Problems and Mitigation

| # | Problem | Possible cause | Impact | Proposed solution / mitigation |
| --- | --- | --- | --- | --- |
| 1 | Duplicate rows or inflated totals after replay | Same batch appended without identity control | Financial and occupancy reports become incorrect | Full refresh plus PK/UQ constraints, locked batch registry, and identical-batch no-op |
| 2 | Snapshot only partly updated | Per-table commits or crash during replacement | Dimensions and facts represent different batches | One publication transaction; all tables and registry commit together or roll back |
| 3 | FK violation during replacement | Wrong insertion/deletion order or missing parent | PostgreSQL rejects statement and aborts transaction | Stage and validate references first; delete children before parents, insert parents before children; preserve valid nulls |
| 4 | Wrong or overwritten input files | Another transformation overwrote date-scoped CSVs | Metadata describes different data from what is loaded | Freeze exact package and verify every file against the selected handover's SHA-256 |
| 5 | Unapproved transformed output loaded | Treating file existence or release flag alone as approval | Warning/rejected data enters reports | Require both decision `APPROVED` and release true, with matching validation evidence |
| 6 | Laptop off, asleep, or rebooted | Local scheduling depends on user hardware | Scheduled run is missed or interrupted | Keep awake during the window; reconcile interrupted attempts and run the latest eligible batch manually |
| 7 | Local PostgreSQL connection fails | Stopped service, wrong port, missing role, container/WSL routing error | Load cannot start | Test connection from the actual execution environment; correct service/routing/privileges; bounded retry only for transient failure |
| 8 | Disk fills during COPY or refresh | Insufficient room for staging, row versions, indexes, WAL, and backup | Statement fails; database may need disk recovery before another run | PRE-08 measured reserve, capacity monitoring, retention after approval, then safe retry after rollback |
| 9 | Locks or overlapping manual runs | Another writer or long transaction holds a conflicting lock | Load exceeds its window or races another batch | Advisory lock, fixed-order writer locks, 30-second lock timeout; no automatic termination of unrelated sessions |
| 10 | Values change during storage | Floating-point money, new rounding, blanket empty-to-null conversion | Totals or text differ despite matching row counts | Exact numeric types, type-aware null handling, explicit columns, and multiset value comparison |
| 11 | Invalid COPY text/null escaping | Passing a raw CSV without matching its serialization rules | Shifted fields or unexpected nulls | Parse Stage 2 CSV, validate, then encode typed COPY input through the driver; reject malformed values |
| 12 | Schema differs after an update | Stage 2 adds columns or target migration is missing | SQL errors or silent data omission | Version and schema preflight; fail before publication; owner applies reviewed target migration |
| 13 | Older batch overwrites current reporting | Manual replay uses an old processing/reference date | Occupancy and bill aging move backward | Compare dates under lock; reject stale publication; historical restoration is a separate owner operation |
| 14 | Connection lost while committing | Process or connection interruption after PostgreSQL accepted commit | Blind retry may misreport or repeat publication | Inspect durable batch registry; mark unresolved outcome `COMMIT_UNKNOWN` and hold replay |
| 15 | Financial history assumed from daily refresh | Users expect historical dimension states | Prior snapshots cannot be queried from current tables | State latest-snapshot scope; retain audit/backup evidence; SCD Type 2 requires a separate upstream/model design |
| 16 | Logs disappear with transaction rollback | Failure evidence written only inside failed transaction | Root cause and recovery state are lost | Independent local event log and separate audit transaction after rollback |

---

## Activity Coverage and Verification Before Completion

This document supplies the proposed specification for all nine lab sections. Completion of the whole activity still requires implementation and evidence. The table prevents a documentation draft from being presented as a verified loader.

| Lab section | Document coverage | Evidence required after implementation |
| --- | --- | --- |
| 1. Source and target | Section 1 | Working local target connection, approved input package, measured volume and duration |
| 2. Target tables | Section 2 | Migration/schema inspection for all 13 tables, keys, constraints, indexes, and control objects |
| 3. Strategy and rules | Section 3 | Successful full refresh, duplicate no-op, changed-snapshot refresh, crash/rollback and commit-recovery behavior |
| 4. Pre-load checks | Section 4 | Logged PASS run and blocked unapproved, incomplete, corrupt, duplicate-conflict, stale, or incompatible input |
| 5. Reconciliation | Section 5 | Actual per-table counts, key/reference/null results, exact money totals, and value-preservation evidence |
| 6. Metadata | Section 6 | Real attempt/table/completion records with linked run IDs and installed versions |
| 7. Retention/access | Section 7 | Restricted access, retention dry run, protected evidence hold, backup and separate-database restore demonstration |
| 8. Acceptance/recovery | Section 8 | Approval, Load warning hold/review, critical rollback, bounded retry, and visible completion/failure reporting |
| 9. Problems/mitigation | Section 9 | Relevant demonstrated failures and recovery observations; remaining untested scenarios labeled |

Before submission, replace the implementation-status notice with an accurate description of what was built and tested, add verified setup and execution commands, record actual counts/timings, and document any remaining limitations. Do not claim that an unimplemented or untested requirement passed.

# Spartment: Operational and Analytical Batch Data Pipeline

## Stage 1: Data Extraction Documentation

### Objective

The objective of the Spartment Data Extraction Stage is to retrieve the operational records required from the existing Spartment Supabase/PostgreSQL database and prepare a documented, traceable, and validated dataset for the next stage of the pipeline: Data Validation and Transformation.

The extraction stage supports Spartment’s room and occupancy monitoring, tenant and lease management, billing and payment monitoring, outstanding-balance tracking, tenant-request monitoring, room inquiries, reporting, dashboards, CSV exports, payment receipts, and administrative decision-making.

> **Stage boundary:** This document defines extraction and extraction-level validation. Cleaning, standardization, joins for business analysis, aggregations, and business transformations belong to the later PostgreSQL SQL transformation stage described in Lab 2.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Item | Description |
| --- | --- |
| **Source system** | Spartment Apartment Management System |
| **Purpose** | Manages apartment buildings, rooms, tenant profiles, leases, tenant assignments, bills, payments, payment corrections, tenant requests, inquiries, and related operational records. |
| **Source database** | Existing Spartment Supabase/PostgreSQL database |
| **Database management system** | PostgreSQL through Supabase |
| **Database type** | Relational database |
| **Source format** | Direct database extraction; CSV, JSON, and Excel are not the source systems. |

The extracted records will support the pipeline described in Lab 2. Python retrieves the selected records, PostgreSQL SQL performs validation and later transformation, and Apache Airflow schedules and coordinates the daily batch process. Processed and summarized results are stored back in the existing Supabase/PostgreSQL database.

### 1.2 Extraction Method

| Item | Specification |
| --- | --- |
| **Extraction tool** | Python |
| **Database connection** | Configured connection to the existing Supabase/PostgreSQL database |
| **Orchestration tool** | Apache Airflow |
| **Extraction type** | Batch extraction |
| **Extraction mode** | Full batch extraction of the selected records on each scheduled run |
| **Schedule** | Daily, managed by Apache Airflow |
| **Incremental window field** | Not defined in Lab 2; no incremental field is assumed |
| **Raw output** | Extracted records prepared for extraction-level validation and handover |

A full batch extraction is used because the Lab 2 pipeline description specifies daily batch processing and the Lab 3 draft does not define a confirmed incremental extraction field or window. The extraction must not assume that `created_at` or `updated_at` is an incremental watermark without a verified implementation decision.

#### Extraction process

1. Apache Airflow starts the scheduled daily batch job.
2. Python connects to the Spartment Supabase/PostgreSQL database using configured connection settings and credentials.
3. Python retrieves the required columns from the selected source tables.
4. The extracted records are retained as raw extracted data for the current pipeline run.
5. Extraction-level checks verify source accessibility, required tables and columns, identifiers, key uniqueness, record counts, and extraction completion.
6. The run metadata, validation results, and any errors are recorded.
7. The extracted dataset and its validation summary are handed over to the next pipeline stage.

The extraction process retrieves and checks data but does not perform business-level cleaning, aggregation, or analytical transformation.

### 1.3 Extraction Schedule

| Item | Specification |
| --- | --- |
| **Frequency** | Daily scheduled batch |
| **Scheduler** | Apache Airflow |
| **DAG name** | Proposed name: `spartment_daily_extraction` |
| **Scheduled time** | Daily; the exact execution time must be configured in Airflow |
| **Retry policy** | Recommended: retry transient connection or query failures up to 3 times with increasing delay |
| **Processing window** | The run’s configured processing date/time; no incremental watermark is defined in Lab 2 |

The DAG name, exact execution time, and retry count above are implementation recommendations for the extraction specification, not claims that the DAG is already deployed.

### 1.4 Extraction Scope

The extraction includes only the fields needed for the operational and analytical pipeline described in Lab 2. The selected fields support room and occupancy monitoring, lease tracking, billing and collection monitoring, tenant assignments, tenant requests, inquiries, payment corrections, and occupancy transitions.

| Table | Data to extract | Purpose |
| --- | --- | --- |
| `profiles` | `id`, `full_name`, `phone`, `role`, `created_at` | Identify tenants/users related to leases and other tenant records. |
| `buildings` | `id`, `name`, `address`, `created_at` | Identify and monitor apartment buildings. |
| `rooms` | `id`, `building_id`, `room_number`, `floor`, `status`, `size_sqm`, `capacity`, `monthly_rate`, `created_at`, `updated_at` | Monitor room availability, occupancy, capacity, and rental rates. |
| `leases` | `id`, `tenant_id`, `room_id`, `move_in_date`, `lease_end_date`, `status`, `created_at` | Monitor tenant leases, room occupancy, and lease status. |
| `bills` | `id`, `lease_id`, `bill_type`, `billing_period`, `due_date`, `rent_amount`, `electricity_amount`, `water_amount`, `status`, `created_at` | Monitor bill periods, due dates, amounts, and bill status. |
| `payments` | `id`, `bill_id`, `amount`, `payment_date`, `reference_note`, `created_at` | Monitor payment transactions and collections. |
| `payment_corrections` | `id`, `payment_id`, `corrected_by`, `reason`, `previous_values`, `corrected_values`, `created_at` | Trace corrections made to payment records. |
| `tenant_assignments` | `id`, `room_id`, `full_name`, `email`, `phone`, `monthly_rent`, `move_in`, `lease_end`, `status`, `created_at` | Monitor tenant-to-room assignments and assignment periods. |
| `tenant_requests` | `id`, `tenant_id`, `room_id`, `kind`, `subject`, `priority`, `status`, `created_at`, `updated_at` | Monitor tenant requests, including maintenance-related requests, and their status. |
| `inquiries` | `id`, `room_id`, `full_name`, `email`, `phone`, `preferred_room`, `move_in_date`, `status`, `created_at` | Monitor room inquiries and prospective tenant information. |
| `tenant_occupancy_events` | `id`, `tenant_id`, `previous_lease_id`, `new_lease_id`, `performed_by`, `created_at` | Track tenant occupancy and lease transitions. |

**Scope notes**

- Table and column names follow the Spartment schema documented in Lab 2. Confirm the actual deployed schema before implementing queries.
- Lab 2 describes maintenance monitoring, while the documented schema includes `tenant_requests`; this extraction treats tenant requests as the available request-tracking table. A separate `maintenance_requests` table should not be assumed unless it exists in the deployed database.
- The `details` column in `tenant_occupancy_events` is not included because the listed extraction scope only needs the event identifiers, lease transition references, actor, and timestamp for occupancy-transition tracking. Add it only if the downstream requirement needs event details.
- Password-recovery and login-attempt control tables are excluded because they are not required for the operational analytics scope: `password_recovery_sessions`, `password_recovery_limits`, and `login_attempt_limits`.
- Notifications are part of the wider Spartment system described in Lab 2 but are not included in this selected extraction scope because the current scope prioritizes room, occupancy, billing, payments, requests, and inquiries. They can be added if a defined reporting requirement needs notification analysis.
- Fields not needed for the stated pipeline objectives should not be extracted.

### 1.5 Date Range and Extraction Window

The initial extraction will retrieve all available records from the selected tables and columns. Subsequent runs will follow the daily full-batch schedule described in Lab 2.

The available Lab 2 and Lab 3 materials do not define a reliable incremental watermark or a specific historical date range. Therefore:

- Do not assume `created_at`, `updated_at`, or another field is an incremental watermark.
- Record the run start and end timestamps for traceability.
- If incremental extraction is implemented later, document and approve the watermark field, window boundaries, late-arriving-record handling, and recovery behavior before enabling it.

### 1.6 Source Limitations and Assumptions

**Limitations**

- Extraction depends on the availability of the existing Supabase/PostgreSQL database and network connection.
- Valid database credentials and sufficient permissions for the selected tables are required.
- Schema changes, renamed columns, or changed data types may cause extraction failure.
- Missing, incomplete, or historically unavailable records may limit reporting.
- Large tables may require pagination or chunked reads to avoid query limits and memory issues.
- Incorrect relationships or orphaned references in source data may be detected during validation and must not be silently corrected during extraction.

**Assumptions**

- The documented Spartment schema in Lab 2 is the reference schema; the deployed schema must be verified before implementation.
- Python is used for extraction and Apache Airflow manages the daily batch schedule.
- PostgreSQL SQL is used for the later validation, cleaning, joins, aggregation, and transformation stages.
- Credentials are stored in a secure server-side configuration or Airflow connection and are not included in source code or logs.
- The existing Supabase/PostgreSQL database is both the source and the destination for processed results, as specified in Lab 2.
- Full batch extraction is used until an incremental strategy is explicitly designed and validated.

---

## 2. Source Tables and Column Specification

This section documents only the selected fields relevant to the Spartment data pipeline. Data types and relationships below follow the schema metadata in Lab 2 and should be checked against the deployed database before coding.

### 2.1 Extraction Tables Overview

| Table | Primary key | Important relationships | Reason for inclusion |
| --- | --- | --- | --- |
| `profiles` | `id` (`uuid`) | `id` references `auth.users.id`; referenced by `leases.tenant_id` and `tenant_requests.tenant_id` | Provides tenant/user identity for lease and request monitoring. |
| `buildings` | `id` (`uuid`) | No foreign-key relationship is defined in the Lab 2 schema | Identifies buildings for operational monitoring and reporting. |
| `rooms` | `id` (`uuid`) | `building_id` → `buildings.id`; referenced by `leases.room_id`, `inquiries.room_id`, `tenant_assignments.room_id`, and `tenant_requests.room_id` | Provides room status, capacity, floor, and rate information. |
| `leases` | `id` (`uuid`) | `tenant_id` → `profiles.id`; `room_id` → `rooms.id` | Connects tenants to rooms and lease periods. |
| `bills` | `id` (`uuid`) | `lease_id` → `leases.id`; referenced by `payments.bill_id` | Provides billing amounts, periods, due dates, and status. |
| `payments` | `id` (`uuid`) | `bill_id` → `bills.id`; referenced by `payment_corrections.payment_id` | Provides payment dates and amounts for collection monitoring. |
| `payment_corrections` | `id` (`uuid`) | `payment_id` → `payments.id`; `corrected_by` → `auth.users.id` | Provides traceability for payment changes. |
| `tenant_assignments` | `id` (`uuid`) | `id` → `auth.users.id` as documented in Lab 2; `room_id` → `rooms.id` | Tracks tenant-to-room assignment information. |
| `tenant_requests` | `id` (`uuid`) | `tenant_id` → `profiles.id`; `room_id` → `rooms.id` | Tracks tenant requests and request status. |
| `inquiries` | `id` (`uuid`) | `room_id` → `rooms.id` | Tracks inquiries associated with rooms. |
| `tenant_occupancy_events` | `id` (`uuid`) | `tenant_id` → `profiles.id`; `previous_lease_id` and `new_lease_id` → `leases.id`; `performed_by` → `profiles.id` | Tracks occupancy and lease transitions. |

### 2.2 Relevant Column Definitions

| Table | Column | Data type in Lab 2 | Key / relationship | Extraction purpose |
| --- | --- | --- | --- | --- |
| `profiles` | `id` | `uuid` | PK; references `auth.users.id` | Unique profile identifier |
| `profiles` | `full_name` | `text` | — | Tenant/user display name |
| `profiles` | `phone` | `text` | — | Contact reference when operationally needed |
| `profiles` | `role` | User-defined `app_role` | — | Distinguishes user role |
| `profiles` | `created_at` | `timestamptz` | — | Record creation trace |
| `buildings` | `id` | `uuid` | PK | Unique building identifier |
| `buildings` | `name` | `text` | — | Building name |
| `buildings` | `address` | `text` | — | Building location |
| `buildings` | `created_at` | `timestamptz` | — | Record creation trace |
| `rooms` | `id` | `uuid` | PK | Unique room identifier |
| `rooms` | `building_id` | `uuid` | FK → `buildings.id` | Associates room with building |
| `rooms` | `room_number` | `text` | — | Room identifier used in operations |
| `rooms` | `floor` | `integer` | — | Room floor |
| `rooms` | `status` | User-defined `room_status` | — | Availability/status reference |
| `rooms` | `size_sqm` | `numeric` | — | Room size |
| `rooms` | `capacity` | `text` | — | Documented room capacity field |
| `rooms` | `monthly_rate` | `numeric` | — | Monthly room rate |
| `rooms` | `created_at`, `updated_at` | `timestamptz` | — | Record timestamps |
| `leases` | `id` | `uuid` | PK | Unique lease identifier |
| `leases` | `tenant_id` | `uuid` | FK → `profiles.id` | Tenant linked to lease |
| `leases` | `room_id` | `uuid` | FK → `rooms.id` | Room linked to lease |
| `leases` | `move_in_date`, `lease_end_date` | `date` | — | Lease period |
| `leases` | `status` | User-defined `lease_status` | — | Lease state |
| `leases` | `created_at` | `timestamptz` | — | Record creation trace |
| `bills` | `id` | `uuid` | PK | Unique bill identifier |
| `bills` | `lease_id` | `uuid` | FK → `leases.id` | Lease billed |
| `bills` | `bill_type` | `text` | — | Bill category |
| `bills` | `billing_period`, `due_date` | `date` | — | Billing period and deadline |
| `bills` | `rent_amount`, `electricity_amount`, `water_amount` | `numeric` | — | Bill components |
| `bills` | `status` | User-defined `bill_status` | — | Bill payment status |
| `bills` | `created_at` | `timestamptz` | — | Record creation trace |
| `payments` | `id` | `uuid` | PK | Unique payment identifier |
| `payments` | `bill_id` | `uuid` | FK → `bills.id` | Bill paid |
| `payments` | `amount` | `numeric` | — | Payment amount |
| `payments` | `payment_date` | `date` | — | Date of payment |
| `payments` | `reference_note` | `text` | — | Payment reference note |
| `payments` | `created_at` | `timestamptz` | — | Record creation trace |
| `payment_corrections` | `id` | `uuid` | PK | Unique correction identifier |
| `payment_corrections` | `payment_id` | `uuid` | FK → `payments.id` | Payment being corrected |
| `payment_corrections` | `corrected_by` | `uuid` | FK → `auth.users.id` | User who made the correction |
| `payment_corrections` | `reason` | `text` | — | Reason for correction |
| `payment_corrections` | `previous_values`, `corrected_values` | `jsonb` | — | Before-and-after values |
| `payment_corrections` | `created_at` | `timestamptz` | — | Correction timestamp |
| `tenant_assignments` | `id` | `uuid` | PK; Lab 2 documents `id` → `auth.users.id` | Assignment/user identifier as defined by source schema |
| `tenant_assignments` | `room_id` | `uuid` | FK → `rooms.id` | Assigned room |
| `tenant_assignments` | `full_name`, `email`, `phone` | `text` | — | Assignment contact details |
| `tenant_assignments` | `monthly_rent` | `numeric` | — | Agreed monthly rent |
| `tenant_assignments` | `move_in`, `lease_end` | `date` | — | Assignment period |
| `tenant_assignments` | `status` | `text` | — | Assignment status |
| `tenant_assignments` | `created_at` | `timestamptz` | — | Record creation trace |
| `tenant_requests` | `id` | `uuid` | PK | Unique request identifier |
| `tenant_requests` | `tenant_id`, `room_id` | `uuid` | FK → `profiles.id`, `rooms.id` | Request owner and related room |
| `tenant_requests` | `kind`, `subject`, `priority`, `status` | `text` | — | Request type, subject, priority, and state |
| `tenant_requests` | `created_at`, `updated_at` | `timestamptz` | — | Request timestamps |
| `inquiries` | `id` | `uuid` | PK | Unique inquiry identifier |
| `inquiries` | `room_id` | `uuid` | FK → `rooms.id` | Room being inquired about |
| `inquiries` | `full_name`, `email`, `phone` | `text` | — | Prospective tenant contact details |
| `inquiries` | `preferred_room`, `status` | `text` | — | Room preference and inquiry state |
| `inquiries` | `move_in_date` | `date` | — | Requested move-in date |
| `inquiries` | `created_at` | `timestamptz` | — | Inquiry timestamp |
| `tenant_occupancy_events` | `id` | `uuid` | PK | Unique occupancy event |
| `tenant_occupancy_events` | `tenant_id` | `uuid` | FK → `profiles.id` | Tenant involved |
| `tenant_occupancy_events` | `previous_lease_id`, `new_lease_id` | `uuid` | FK → `leases.id` | Previous and new lease references |
| `tenant_occupancy_events` | `performed_by` | `uuid` | FK → `profiles.id` | Actor recorded by source schema |
| `tenant_occupancy_events` | `created_at` | `timestamptz` | — | Event timestamp |

**Schema verification note:** The data types and relationships above reproduce the Lab 2 schema metadata. In particular, `tenant_assignments.id` and `tenant_occupancy_events.performed_by` should be checked against the live database because their documented relationships should match the actual implementation before extraction is deployed.

---

## 3. Extraction Validation and Data Quality Checks

These checks determine whether the extracted data is complete and structurally suitable for handover. They do not clean or transform business data.

| Check name | Target | Purpose | Validation criteria |
| --- | --- | --- | --- |
| Source connection and accessibility | Supabase/PostgreSQL connection | Confirm the source is reachable before extraction | **PASS:** connection succeeds. **FAIL:** connection or authentication error; record the error and stop the run. |
| Required tables exist | All 11 selected tables in §1.4 | Detect missing or renamed tables | **PASS:** all selected tables are accessible. **FAIL:** any required table is missing or inaccessible. |
| Required columns exist | Selected columns listed in §1.4 and §2 | Detect schema changes before querying data | **PASS:** all required columns exist. **FAIL:** any required column is missing or has an incompatible schema. |
| Required identifiers are present | Primary keys and selected foreign-key columns | Ensure records can be traced and related downstream | **PASS:** required identifiers are not null. **FAIL:** one or more required identifiers are null; affected extract is held for review or quarantined. |
| Primary-key uniqueness | `id` for each selected table | Detect duplicate identifiers in the extracted dataset | **PASS:** no duplicate primary keys. **FAIL:** duplicate primary keys are found. |
| Record count and completeness | Extracted records per table | Confirm that the query result was fully retrieved | **PASS:** extracted count matches the source count for the same extraction scope. **WARNING:** counts differ and require investigation. **FAIL:** query was interrupted or the extract is clearly incomplete. |
| Referential integrity check | Selected foreign-key references | Detect references to missing related records | **PASS:** no orphaned references are found in the extracted scope. **WARNING/FAIL:** orphaned references are found; record the affected table and count and hold them for review. |
| Extraction interruption/error | Python extraction job and Airflow task | Identify partial runs and failed queries | **PASS:** task completes successfully. **FAIL:** exception, timeout, or incomplete retrieval; retry according to the configured policy. |
| Empty-result check | Each selected table | Distinguish a legitimately empty table from a failed or unexpectedly incomplete extract | **PASS:** zero records are consistent with the source count. **WARNING:** source has records but the extract is empty. |
| Data type/readability check | Selected columns | Ensure values can be read using the documented schema | **PASS:** values are returned in compatible types. **FAIL:** query or conversion errors prevent a reliable extract; do not silently coerce values. |

### Validation scope

- Extraction checks verify accessibility, schema, identifiers, uniqueness, counts, and successful retrieval.
- Business rules such as calculating occupancy, summing billing components, reconciling payments, standardizing statuses, removing duplicates from source data, and calculating outstanding balances belong to the PostgreSQL SQL transformation stage in Lab 2.
- A validation failure must be logged and must not be hidden by silently changing source values.

---

## 4. Extraction Metadata and Log Specification

Each run should produce a traceable record for every extracted table. These fields are proposed logging requirements for the pipeline.

| Field | Purpose | Example |
| --- | --- | --- |
| `pipeline_run_id` | Uniquely identifies the batch run | `spartment-extract-2026-10-14-0100` |
| `table_name` | Identifies the extracted table | `rooms` |
| `extraction_started_at` | Records when extraction started | `2026-10-14T01:00:00+08:00` |
| `extraction_ended_at` | Records when extraction ended | `2026-10-14T01:02:15+08:00` |
| `extraction_status` | Records the final result | `SUCCESS`, `WARNING`, or `FAILED` |
| `records_read` | Number of records returned/read from the source | `350` |
| `records_extracted` | Number of records successfully placed in raw output | `350` |
| `records_rejected` | Number of records held or rejected by extraction-level checks, if applicable | `0` |
| `extraction_window_start` | Start of the configured extraction period, if applicable | `2026-10-14T01:00:00+08:00` |
| `extraction_window_end` | End of the configured extraction period, if applicable | `2026-10-14T01:02:15+08:00` |
| `validation_result` | Summarizes the extraction checks | `PASS` |
| `error_message` / `error_code` | Captures failure details when applicable | `null` or `DB_CONNECTION_TIMEOUT` |
| `extraction_duration_ms` | Measures elapsed extraction time | `135000` |

### Example log record

```json
{
  "pipeline_run_id": "spartment-extract-2026-10-14-0100",
  "table_name": "rooms",
  "extraction_started_at": "2026-10-14T01:00:00+08:00",
  "extraction_ended_at": "2026-10-14T01:02:15+08:00",
  "extraction_status": "SUCCESS",
  "records_read": 350,
  "records_extracted": 350,
  "records_rejected": 0,
  "extraction_window_start": null,
  "extraction_window_end": null,
  "validation_result": "PASS",
  "error_message": null,
  "extraction_duration_ms": 135000
}
```

The example timestamps are illustrative, not actual execution results. Since the documented pipeline uses full batch extraction and does not define an incremental watermark, the extraction-window fields may be null or may record the run boundaries rather than an incremental data window.

### How logs are used

- **Monitoring:** Check Airflow task status, per-table results, duration, and record counts.
- **Troubleshooting:** Identify the table, run, and error that caused a failure.
- **Auditing:** Trace when a dataset was extracted and which validation checks were performed.
- **Recovery:** Determine whether a run completed and which tables need to be safely re-extracted.

Logs must not contain database passwords, service-role keys, or other secrets.

---

## 5. Log Retention and Access

The following is a proposed retention policy for the Spartment extraction pipeline and should be approved by the system owner before implementation.

| Item | Policy |
| --- | --- |
| **Operational log retention** | Retain detailed extraction logs for at least 90 days to support troubleshooting and routine monitoring. |
| **Audit summary retention** | Retain run summaries, counts, status, and validation results for at least 1 year, subject to applicable institutional and organizational requirements. |
| **Storage location** | Apache Airflow task logs and an approved centralized log store or PostgreSQL logging table. Raw extracted datasets should be kept in a designated staging location until handover and recovery requirements are satisfied. |
| **Access permissions** | Limit log access to authorized administrators and personnel responsible for data-pipeline maintenance. Restrict write access to the pipeline service account and authorized maintainers. |
| **Credentials** | Do not store credentials, access tokens, or service-role keys in logs. Use secured Airflow connections or protected environment configuration. |
| **Archiving** | Archive logs required for audit or recovery before removing them from active storage. Preserve the run ID and date so archived logs can be found. |
| **Deletion rules** | Do not delete logs before the retention period expires or while they are required for troubleshooting, auditing, or recovery. Deletion after the retention period should follow an approved process and be recorded. |
| **Responsible role** | The authorized system administrator or designated data-pipeline maintainer manages retention, access, archiving, and deletion. |

Operational logs and audit summaries may have different retention periods. Any stricter institutional, legal, or organizational retention rule takes precedence over this proposed policy.

---

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Schema

The extracted dataset must preserve the selected source table names, column names, data types, and key relationships documented in Lab 2.

Required tables:

- `profiles`
- `buildings`
- `rooms`
- `leases`
- `bills`
- `payments`
- `payment_corrections`
- `tenant_assignments`
- `tenant_requests`
- `inquiries`
- `tenant_occupancy_events`

Required identifiers include each table’s documented primary key and the selected foreign-key fields needed to relate records. Refer to §2 for the relevant fields and relationships.

### 6.2 Naming Convention

- Preserve source table and column names unless a documented mapping is approved.
- Use the source schema’s `snake_case` names, such as `room_number`, `lease_end_date`, `billing_period`, and `payment_date`.
- Keep primary-key columns named `id` where that is how the source schema defines them.
- Preserve foreign-key names such as `building_id`, `tenant_id`, `room_id`, `lease_id`, `bill_id`, and `payment_id`.
- Do not rename columns or abbreviate names without documenting the mapping.
- Preserve PostgreSQL types where possible, including `uuid`, `text`, `integer`, `numeric`, `date`, `timestamptz`, and `jsonb`.

### 6.3 Schema Consistency

Before extraction, verify that all required tables and columns exist and match the expected schema.

1. If a required table or column is missing, mark the run as failed and do not silently omit the field.
2. If a data type or relationship has changed, record the difference and hold the affected extract for review.
3. Update this data contract and the extraction query only after the schema change is confirmed.
4. Re-run the affected extraction after the mapping or query is corrected.
5. Preserve the run metadata so the failed attempt and recovery attempt remain traceable.

### 6.4 Source-to-Standardized Field Mapping

The current extraction contract keeps the native Spartment field names. No renaming is specified in Lab 2.

| Source column | Standardized column | Source type | Target type | Mapping type |
| --- | --- | --- | --- | --- |
| `rooms.room_number` | `room_number` | `text` | `text` | Direct |
| `leases.move_in_date` | `move_in_date` | `date` | `date` | Direct |
| `bills.billing_period` | `billing_period` | `date` | `date` | Direct |
| `payments.payment_date` | `payment_date` | `date` | `date` | Direct |
| `*.created_at` | `created_at` | `timestamptz` | `timestamptz` | Direct |
| `payment_corrections.previous_values` | `previous_values` | `jsonb` | `jsonb` | Direct |
| `payment_corrections.corrected_values` | `corrected_values` | `jsonb` | `jsonb` | Direct |

If implementation reveals that a source column differs from this contract, document the actual source type and the approved mapping rather than assuming the schema is unchanged.

---

## 7. Extraction Acceptance and Handover Rules

The extraction stage must decide whether its output is safe to pass to the next stage. The decision is based on the extraction-level checks in §3.

### 7.1 Decision Rules

| Overall condition | Acceptance decision | Pipeline action |
| --- | --- | --- |
| All required tables and columns are available; required identifiers and keys pass; extraction completes; record counts reconcile | **APPROVED** | Pass the extracted raw datasets, metadata, and validation summary to the next stage. |
| Extraction completes but a non-critical warning remains, such as an unexpected record count that needs confirmation | **MANUAL REVIEW** | Hold handover until an authorized maintainer reviews and records the decision. |
| A critical check fails, such as a missing required table, inaccessible source, duplicate primary keys, or interrupted/incomplete extraction | **REJECTED** | Stop promotion to the next stage; retry transient failures or quarantine the affected output for investigation. |

### 7.2 Required Decisions

**Automatic approval**

The dataset may proceed automatically when:

- The source connection succeeds.
- All required tables and columns exist.
- Required identifiers are present.
- Primary-key uniqueness checks pass.
- Extraction completes without an unhandled error.
- Record counts are reconciled for the extraction scope.
- No unresolved critical validation failure remains.

**Manual review**

Manual review is required when:

- Record counts differ from the expected source count.
- A non-critical validation warning needs explanation.
- A relationship or source-schema discrepancy requires confirmation.
- The source returns an unexpected empty result.

The designated system administrator or data-pipeline maintainer reviews the logs and approves or rejects the handover. A warning must not be silently treated as a pass.

**Rejection or failure**

The extract is rejected or held when:

- The source database cannot be reached after configured retries.
- A required table or column is missing.
- A primary key is duplicated.
- Required identifiers are missing.
- The query fails or the extracted dataset is incomplete.
- A critical schema mismatch prevents reliable interpretation.

Transient connection or query failures may be retried according to the Airflow configuration. If retries fail, the pipeline stops handover and records the error. The affected output is quarantined or discarded according to the approved recovery procedure, and the extraction is run again after the issue is resolved.

### 7.3 Handover to the Transformation Stage

The next stage in Lab 2 uses PostgreSQL SQL to validate, clean, join, aggregate, and summarize the extracted Spartment records. The extraction handover should contain:

- Raw extracted datasets for the selected tables.
- The table/column contract and extraction run ID.
- Per-table record counts and extraction timestamps.
- Validation results (`PASS`, `WARNING`, or `FAIL`).
- Error details and the location/reference of any quarantined output.
- The configured processing period, if one is set.

No claim is made that a particular file format or staging directory is already implemented. The project must select and document the actual staging format and location before deployment.

#### Example validation summary

| Validation summary | Result |
| --- | --- |
| `profiles` extraction validation | PASS |
| `rooms` extraction validation | PASS |
| `leases` extraction validation | PASS |
| `bills` extraction validation | PASS |
| `payments` extraction validation | PASS |
| Overall extraction | APPROVED |

*This is an illustrative example only, not a report of an actual pipeline run.*

---

## 8. Common Extraction Problems and Mitigation

| # | Problem description | Possible cause | Impact on extraction | Proposed solution / mitigation |
| --- | --- | --- | --- | --- |
| 1 | Supabase/PostgreSQL is unreachable | Network interruption, database maintenance, incorrect credentials, or unavailable service | The extraction cannot retrieve records and downstream stages cannot proceed | Retry transient failures using the configured Airflow policy; log the error; verify connectivity and credentials securely; stop handover if retries fail. |
| 2 | Required table or column is missing | Database migration, renamed column, or mismatch between Lab 2 documentation and the deployed schema | Queries fail or the dataset becomes incomplete | Run schema checks before extraction; stop the affected task; confirm the deployed schema; update the contract and query only after review. |
| 3 | Extracted record count does not match the source | Interrupted query, pagination error, query filter mistake, or concurrent source changes during extraction | Reports may be based on incomplete records | Record source and extracted counts for the same scope; use consistent query boundaries; investigate and re-run the affected table before approval. |
| 4 | Duplicate primary keys appear in the extract | Repeated page retrieval, incorrect result combination, or unexpected source data | Records may be counted more than once or produce unreliable downstream joins | Check primary-key uniqueness; log affected identifiers; hold or quarantine the output; correct the extraction logic or investigate the source before retrying. |
| 5 | Required identifier is null | Incomplete source record or schema/data-integrity issue | The record may not be traceable or link correctly to related records | Detect and count null identifiers; retain evidence in validation logs; quarantine or hold affected records for review rather than silently inventing identifiers. |
| 6 | Related record is missing | A foreign-key reference points to a record not present in the extracted scope or source data | Related records may not be linkable during transformation | Check relevant foreign-key references; verify whether the referenced table/record is in scope; hold unresolved critical cases and document the result for the transformation stage. |
| 7 | Large table extraction times out or exceeds memory | Too many rows fetched at once, query inefficiency, or connection limits | The run may fail or produce a partial dataset | Use bounded queries or pagination, fetch records in manageable batches, monitor duration, and verify final counts before marking the extract successful. |
| 8 | Sensitive credentials are exposed in logs or code | Credentials are hard-coded or printed during debugging | Unauthorized database access may occur | Store credentials in secured Airflow connections or protected environment configuration; redact secrets; rotate exposed credentials and review access if an exposure occurs. |
| 9 | Unexpected empty result | Wrong schema/table, query condition error, permissions issue, or genuinely empty source table | The pipeline may incorrectly treat missing data as valid | Compare the extract result with the source count and expected table availability; record a warning or fail the run if the empty result is unexplained. |
| 10 | Source schema changes during a run | Application deployment or migration occurs while extraction is running | A query may fail or return data inconsistent with the documented contract | Coordinate extraction with schema changes where possible; fail fast on schema mismatch; version the contract and re-run after compatibility is confirmed. |

---

## Expected Output

At the end of Stage 1, the team should have:

1. The raw extracted data for the selected Spartment operational tables.
2. A record of the extraction run, timestamps, counts, and status.
3. Extraction-level validation results.
4. A documented data contract and naming convention.
5. A clear acceptance decision and handover package for the PostgreSQL SQL transformation stage.

The extraction documentation must remain consistent with the actual implementation. Recommendations such as the proposed Airflow DAG name, retry policy, and retention periods must be confirmed during implementation. Data cleaning, standardization, aggregation, and business transformations remain part of the later stages defined in Lab 2.


# Spartment: Operational and Analytical Batch Data Pipeline

## Stage 1: Data Extraction Documentation

### Objective

The objective of the Spartment Data Extraction Stage is to retrieve the operational records needed from the Spartment Supabase/PostgreSQL database and prepare a documented, traceable, and validated dataset for the next stages of the data pipeline.

The extracted data supports room availability and occupancy monitoring, tenant assignment tracking, billing monitoring, prospective tenant inquiries, and tenant request analysis.

> The goal of this stage is to extract the required operational data and prepare it for validation and transformation in the next pipeline stage.

---

## 1. Data Source and Extraction Specification

### 1.1 Source System

| Item | Description |
| --- | --- |
| **Source System** | Spartment Apartment Management System |
| **Purpose** | Manages apartment buildings and rooms, tenant assignments, billing records, prospective tenant inquiries, and tenant requests. These records support operational reporting and decision-making. |
| **Source Database** | Supabase PostgreSQL |
| **Database management system** | PostgreSQL (via Supabase) |
| **File format** | Not applicable — direct database extraction; the source is not a CSV, JSON, or Excel file. |

### 1.2 Extraction Method

| Item | Description |
| --- | --- |
| **Extraction tool** | Python |
| **Orchestration tool** | Apache Airflow |
| **Extraction type** | Scheduled batch extraction |
| **Extraction mode** | Full extraction for the initial load; incremental extraction only after a reliable change-tracking field and recovery logic are implemented |
| **Raw staging** | `rawdata/` staging area, with one dataset per selected table and run date |

The extraction process follows this sequence:

1. Apache Airflow starts the scheduled extraction job.
2. Python connects to the Spartment Supabase/PostgreSQL database using protected server-side credentials.
3. Python fetches the required columns from the selected operational tables.
4. Extracted records are placed in the `rawdata` staging area.
5. Extraction-level validation checks schema, identifiers, uniqueness, record counts, and successful retrieval.
6. The raw data, extraction metadata, and validation results are passed to the next pipeline stage.

Extraction is limited to retrieval and extraction-level checks. Data cleaning, standardization, joins for analysis, and business calculations are performed in the transformation stage.

### 1.3 Extraction Window

The initial load retrieves all available records from the selected tables. A reliable incremental watermark has not yet been defined for every selected table, so incremental extraction must not be enabled until the appropriate fields and recovery rules are confirmed.

| Table | Candidate field | Extraction approach |
| --- | --- | --- |
| `rooms` | `updated_at` | Use for incremental extraction only if updates are reliably maintained for all relevant changes. |
| `tenant_requests` | `updated_at` | Use for incremental extraction only if updates are reliably maintained for all relevant changes. |
| `tenant_assignments` | `updated_at` | Use full extraction unless a reliable change-tracking field is added and verified. |
| `bills` | `updated_at` | Use full extraction unless a reliable change-tracking field is added and verified. |
| `inquiries` | `created_at` | Can identify newly created inquiries; it does not capture later status changes unless a reliable update field is added. |
| `tenant_occupancy_events` | `created_at` | Can identify newly created events; use a safe window and deduplication strategy if incremental extraction is implemented. |
| `profiles` | `created_at` | Can identify newly created profiles; it does not capture later profile changes unless an update field is available. |
| `buildings` | `created_at` | Can identify newly created buildings; it does not capture later changes unless an update field is available. |

For incremental extraction, use a stored successful-run watermark, an overlap window where appropriate, and primary-key-based deduplication. Update the watermark only after extraction and validation succeed. Do not assume that a candidate field captures every change unless the application and database maintain it reliably.

### 1.4 Extraction Schedule

| Item | Value |
| --- | --- |
| Frequency | Daily scheduled batch |
| Airflow DAG | `spartment_daily_extraction` |
| Scheduled time | Once daily during off-peak hours, for example 01:00 Asia/Manila |
| Retry | Up to 3 retries with backoff for temporary connection or query failures |

The DAG name, schedule, and retry count are configuration recommendations and should match the implemented Airflow pipeline.

### 1.5 Extraction Scope

The extraction focuses on operational records that are directly useful for the Spartment pipeline.

| Table | Data to extract | Purpose |
| --- | --- | --- |
| `profiles` | `id`, `full_name`, `phone`, `role`, `created_at` | Identify tenant/user records needed to interpret assignments and requests. |
| `buildings` | `id`, `name`, `address`, `created_at` | Identify buildings for room-level and building-level reporting. |
| `rooms` | `id`, `building_id`, `room_number`, `floor`, `status`, `size_sqm`, `capacity`, `monthly_rate`, `created_at`, `updated_at` | Monitor room availability, capacity, location, and rental rates. |
| `tenant_assignments` | `id`, `room_id`, `full_name`, `email`, `phone`, `monthly_rent`, `move_in`, `lease_end`, `status`, `created_at` | Monitor tenant-to-room assignments and occupancy periods. |
| `bills` | `id`, `lease_id`, `bill_type`, `billing_period`, `due_date`, `rent_amount`, `electricity_amount`, `water_amount`, `status`, `created_at` | Monitor billing periods, bill components, due dates, and bill status. |
| `inquiries` | `id`, `room_id`, `full_name`, `email`, `phone`, `preferred_room`, `move_in_date`, `status`, `created_at` | Monitor prospective tenant inquiries and room interest. |
| `tenant_requests` | `id`, `tenant_id`, `room_id`, `kind`, `subject`, `priority`, `status`, `created_at`, `updated_at` | Monitor tenant requests, priorities, and request status. |
| `tenant_occupancy_events` | `id`, `tenant_id`, `previous_lease_id`, `new_lease_id`, `performed_by`, `created_at` | Track occupancy or lease-transition events. |

> **Scope note:** Only extract fields required by the current pipeline objectives. Sensitive authentication and password-recovery data, such as `password_recovery_sessions`, `password_recovery_limits`, and `login_attempt_limits`, is out of scope. Additional tables should be included only when a defined analytics requirement needs them.

### 1.6 Source Limitations and Assumptions

**Limitations**

- Extraction depends on the availability of the Supabase/PostgreSQL database and network connection.
- Valid database credentials and sufficient permissions are required.
- Schema changes may cause extraction queries to fail.
- Some tables do not have a confirmed update timestamp, which limits safe incremental extraction.
- Missing or incomplete source records may affect downstream analysis.
- Large tables may require pagination or chunked extraction.

**Assumptions**

- The current Supabase schema is the source of truth and is verified before the extractor is deployed.
- Python performs extraction, and Apache Airflow schedules and monitors the batch process.
- Database credentials are stored in a protected server-side configuration or Airflow connection and are never written to logs or frontend code.
- The raw extracted data is kept separate from transformed analytical data.
- Full extraction is used where reliable incremental tracking is unavailable.
- The schema diagram includes billing fields referencing `lease_id`, but a `leases` table is not shown in the supplied schema diagram. Verify the deployed database and relationship before implementing joins that require it.

---

## 2. Source Tables and Column Specification

> Include only columns and details relevant to the pipeline. The fields below follow the supplied schema diagram and must be verified against the deployed database.

### 2.1 Extraction Tables Overview

| Table | Data to extract | Purpose |
| --- | --- | --- |
| `profiles` | `id`, `full_name`, `phone`, `role`, `created_at` | Tenant/user identification for operational records. |
| `buildings` | `id`, `name`, `address`, `created_at` | Building identification and reporting. |
| `rooms` | `id`, `building_id`, `room_number`, `floor`, `status`, `size_sqm`, `capacity`, `monthly_rate`, `created_at`, `updated_at` | Room status and rental information. |
| `tenant_assignments` | `id`, `room_id`, `full_name`, `email`, `phone`, `monthly_rent`, `move_in`, `lease_end`, `status`, `created_at` | Tenant assignment and occupancy tracking. |
| `bills` | `id`, `lease_id`, `bill_type`, `billing_period`, `due_date`, `rent_amount`, `electricity_amount`, `water_amount`, `status`, `created_at` | Billing and payment-obligation monitoring. |
| `inquiries` | `id`, `room_id`, `full_name`, `email`, `phone`, `preferred_room`, `move_in_date`, `status`, `created_at` | Inquiry and prospective tenant monitoring. |
| `tenant_requests` | `id`, `tenant_id`, `room_id`, `kind`, `subject`, `priority`, `status`, `created_at`, `updated_at` | Tenant request monitoring. |
| `tenant_occupancy_events` | `id`, `tenant_id`, `previous_lease_id`, `new_lease_id`, `performed_by`, `created_at` | Occupancy transition history. |

### 2.2 Relevant Column Definitions

#### `profiles`

**Purpose:** Stores profile information used to identify users involved in operational records.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique profile identifier |
| `full_name` | `TEXT` | — | Profile name |
| `phone` | `TEXT` | — | Contact reference |
| `role` | `app_role` | — | User role |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |

#### `buildings`

**Purpose:** Stores apartment building information.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique building identifier |
| `name` | `TEXT` | — | Building name |
| `address` | `TEXT` | — | Building address |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |

#### `rooms`

**Purpose:** Stores room details used for availability and rental monitoring.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique room identifier |
| `building_id` | `UUID` | Relationship to `buildings.id` | Building that contains the room |
| `room_number` | `TEXT` | — | Room number |
| `floor` | `INTEGER` | — | Floor location |
| `status` | `room_status` | — | Room status |
| `size_sqm` | `NUMERIC` | — | Room size in square meters |
| `capacity` | `TEXT` | — | Room capacity as defined in the schema |
| `monthly_rate` | `NUMERIC` | — | Monthly room rate |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | — | Last update timestamp |

#### `tenant_assignments`

**Purpose:** Stores tenant-to-room assignment and occupancy information.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Assignment identifier as defined by the schema |
| `room_id` | `UUID` | Relationship to `rooms.id` | Assigned room |
| `full_name` | `TEXT` | — | Tenant name |
| `email` | `TEXT` | — | Tenant email |
| `phone` | `TEXT` | — | Tenant phone number |
| `monthly_rent` | `NUMERIC` | — | Monthly rent |
| `move_in` | `DATE` | — | Move-in date |
| `lease_end` | `DATE` | — | Lease end date |
| `status` | `TEXT` | — | Assignment status |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |

#### `bills`

**Purpose:** Stores billing records for apartment charges.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique bill identifier |
| `lease_id` | `UUID` | Relationship to lease record, if present in deployed schema | Lease associated with the bill |
| `bill_type` | `TEXT` | — | Bill category |
| `billing_period` | `DATE` | — | Billing period |
| `due_date` | `DATE` | — | Payment due date |
| `rent_amount` | `NUMERIC` | — | Rent amount |
| `electricity_amount` | `NUMERIC` | — | Electricity charge |
| `water_amount` | `NUMERIC` | — | Water charge |
| `status` | `bill_status` | — | Bill status |
| `created_at` | `TIMESTAMPTZ` | — | Record creation timestamp |

#### `inquiries`

**Purpose:** Stores inquiries from prospective tenants.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique inquiry identifier |
| `room_id` | `UUID` | Relationship to `rooms.id` | Room associated with the inquiry |
| `full_name` | `TEXT` | — | Inquirer's name |
| `email` | `TEXT` | — | Inquirer's email |
| `phone` | `TEXT` | — | Inquirer's phone number |
| `preferred_room` | `TEXT` | — | Preferred room |
| `move_in_date` | `DATE` | — | Requested move-in date |
| `status` | `TEXT` | — | Inquiry status |
| `created_at` | `TIMESTAMPTZ` | — | Inquiry creation timestamp |

#### `tenant_requests`

**Purpose:** Stores tenant requests, including maintenance-related requests if recorded in this table.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique request identifier |
| `tenant_id` | `UUID` | Relationship to `profiles.id` | Tenant who submitted the request |
| `room_id` | `UUID` | Relationship to `rooms.id` | Related room |
| `kind` | `TEXT` | — | Request category |
| `subject` | `TEXT` | — | Request subject |
| `priority` | `TEXT` | — | Request priority |
| `status` | `TEXT` | — | Request status |
| `created_at` | `TIMESTAMPTZ` | — | Request creation timestamp |
| `updated_at` | `TIMESTAMPTZ` | — | Last update timestamp |

#### `tenant_occupancy_events`

**Purpose:** Records tenant occupancy or lease-transition events.

| Column | Data Type | Key | Purpose |
| --- | --- | --- | --- |
| `id` | `UUID` | PK | Unique event identifier |
| `tenant_id` | `UUID` | Relationship to `profiles.id` | Tenant involved in the event |
| `previous_lease_id` | `UUID` | Lease reference, if present | Previous lease |
| `new_lease_id` | `UUID` | Lease reference, if present | New lease |
| `performed_by` | `UUID` | Actor reference as defined in the schema | User who performed the action |
| `created_at` | `TIMESTAMPTZ` | — | Event timestamp |

**Schema verification note:** The supplied schema diagram shows the fields above but does not show all referenced tables. Confirm foreign-key constraints and actual data types in the live database before implementing queries.

---

## 3. Extraction Validation and Data Quality Checks

The checks below cover extraction-level validation only. Data cleaning, standardization, and business transformations are handled in the transformation stage.

| Check Name | Target | Purpose | Validation Criteria (Pass / Fail) |
| --- | --- | --- | --- |
| Source connection and accessibility | Supabase/PostgreSQL connection | Confirm the source is reachable | PASS: connection succeeds; FAIL: connection error or timeout → log error and stop the run |
| Required tables and columns exist | Selected tables and columns in 2 | Detect schema changes | PASS: all required tables and columns exist; FAIL: any required table or column is missing |
| Required identifiers present | Primary keys and required relationship fields | Ensure records can be traced and related | PASS: no null required identifiers; FAIL: null required identifiers → flag or quarantine affected records |
| Primary key uniqueness | `id` in each selected table | Detect duplicate identifiers | PASS: no duplicate primary keys; FAIL: duplicates found → fail table validation |
| Record count and completeness | Extracted rows compared with source rows for the same scope | Detect incomplete extraction | PASS: counts match; WARNING: unexplained count difference; FAIL: interrupted or clearly incomplete extraction |
| Referential completeness | Selected room, tenant, building, and lease references | Detect orphaned references | PASS: references resolve where the related table is available; WARNING/FAIL: unresolved references are recorded and reviewed |
| Numeric range sanity | `monthly_rate`, `monthly_rent`, `rent_amount`, `electricity_amount`, `water_amount` | Flag obviously invalid negative amounts | PASS: no unexpected negative values; WARNING: violations flagged for the transformation stage |
| Date sanity | `move_in`, `lease_end`, `billing_period`, `due_date`, `move_in_date` | Flag invalid or inconsistent dates | PASS: dates can be read and parsed; WARNING: suspicious date relationships are flagged for review |
| Extraction errors or interruptions | Python extraction task / Airflow task | Identify partial or failed runs | PASS: task completes without error; FAIL: exception or timeout → retry according to 1.4 |

---

## 4. Extraction Metadata and Log Specification

Each extraction run should record the following fields for monitoring, troubleshooting, auditing, and recovery.

| Field | Purpose | Example value / format |
| --- | --- | --- |
| `pipeline_run_id` | Unique identifier for the extraction run | `spartment-extract-2026-10-14-0100` |
| `table_name` | Table extracted | `rooms` |
| `extraction_started_at` | Extraction start timestamp | `2026-10-14T01:00:00+08:00` |
| `extraction_ended_at` | Extraction end timestamp | `2026-10-14T01:02:15+08:00` |
| `extraction_status` | Final table/run status | `SUCCESS`, `WARNING`, or `FAILED` |
| `records_read` | Number of records read from source | `350` |
| `records_extracted` | Number of records written to staging | `350` |
| `records_rejected` | Number of records flagged or quarantined, if applicable | `0` |
| `extraction_window_start` | Start of the extraction window, if configured | `null` for full extraction |
| `extraction_window_end` | End of the extraction window or run | `2026-10-14T01:02:15+08:00` |
| `validation_result` | Summary of validation checks | `PASS` |
| `error_message` / `error_code` | Failure detail, if applicable | `null` or `DB_CONNECTION_TIMEOUT` |
| `extraction_duration_ms` | Total extraction duration in milliseconds | `135000` |

Example log record (JSON):

Example log record (JSON):

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
  "extraction_window_end": "2026-10-14T01:02:15+08:00",
  "validation_result": "PASS",
  "error_message": null,
  "extraction_duration_ms": 135000
}

The example values are illustrative and are not actual execution results.

**Usage:**

- **Monitoring:** Check Airflow task status and per-table extraction results.
- **Troubleshooting:** Use run IDs, counts, and error messages to identify failures.
- **Auditing:** Record when data was extracted and which validation checks were performed.
- **Recovery:** Determine whether a table must be re-extracted after a failed or incomplete run.

Never write database passwords, access tokens, or service-role keys to logs.

---

## 5. Log Retention and Access

| Item | Policy |
| --- | --- |
| **Retention period** | Recommended minimum of 90 days for operational logs and 1 year for audit summaries, subject to approved organizational policy. |
| **Storage location** | Airflow task logs and an approved centralized log store or PostgreSQL log table. Raw data is stored separately in `rawdata/`. |
| **Access permissions** | Restrict access to authorized administrators and pipeline maintainers. Write access is limited to the pipeline service account and authorized maintainers. |
| **Archiving** | Archive logs required for auditing or recovery before removing them from active storage. Preserve the run ID and date for lookup. |
| **Deletion rules** | Do not delete logs before the retention period expires or while they are needed for troubleshooting, auditing, or recovery. Record approved deletion after the retention period. |
| **Credentials** | Credentials and service-role keys must never be stored in logs. |

These retention periods are recommendations and should be approved before implementation.

---

## 6. Extraction Data Contract and Naming Convention

### 6.1 Required Schema

The extraction contract includes only the selected operational tables:

- `profiles`
- `buildings`
- `rooms`
- `tenant_assignments`
- `bills`
- `inquiries`
- `tenant_requests`
- `tenant_occupancy_events`

Required identifiers include each selected table's primary key and the relationship fields needed by the pipeline.

### 6.2 Naming Convention

- Use the source table and column names as defined in the database.
- Use `snake_case`, such as `room_number`, `monthly_rate`, `billing_period`, and `tenant_id`.
- Preserve the source identifier name `id` for primary keys.
- Preserve relationship fields such as `building_id`, `room_id`, `tenant_id`, and `lease_id`.
- Do not rename or abbreviate fields without documenting the mapping.
- Preserve PostgreSQL data types such as `uuid`, `text`, `integer`, `numeric`, `date`, `timestamptz`, and custom enum types.

### 6.3 Schema Consistency

Unexpected changes to table names, column names, or data types are handled as follows:

1. The required-table-and-column check fails before the affected dataset is promoted.
2. The pipeline logs the discrepancy and holds or quarantines the affected extract.
3. The source schema is checked and the data contract is updated only after the change is confirmed.
4. The extraction query is corrected and the affected dataset is re-extracted.
5. The failed run and recovery run remain traceable through their run IDs.

### 6.4 Source-to-Standardized Field Mapping

The extraction keeps the native Spartment column names unless a documented transformation requirement says otherwise.

| Source Column | Standardized Column | Source Type | Target Type | Mapping Type |
| --- | --- | --- | --- | --- |
| `rooms.room_number` | `room_number` | `TEXT` | `TEXT` | Direct |
| `rooms.monthly_rate` | `monthly_rate` | `NUMERIC` | `NUMERIC` | Direct |
| `tenant_assignments.move_in` | `move_in` | `DATE` | `DATE` | Direct |
| `tenant_assignments.lease_end` | `lease_end` | `DATE` | `DATE` | Direct |
| `bills.billing_period` | `billing_period` | `DATE` | `DATE` | Direct |
| `*.created_at` | `created_at` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Direct |
| `tenant_requests.updated_at` | `updated_at` | `TIMESTAMPTZ` | `TIMESTAMPTZ` | Direct |

---

## 7. Extraction Acceptance and Handover Rules

Each rule defines the validation outcome, acceptance decision, pipeline action, and responsibility for manual review.

### 7.1 Decision Rules

| Overall Condition | Acceptance Decision | Pipeline Action |
| --- | --- | --- |
| Required tables and columns exist, extraction completes, keys are valid, and record counts reconcile | **APPROVED** | Pass raw data, metadata, and validation results to Transformation |
| A non-critical warning remains, such as suspicious values or an unexplained count difference | **MANUAL REVIEW** | Hold promotion until the warning is reviewed |
| A critical check fails, such as a missing required table, duplicate primary key, or incomplete extraction | **REJECTED** | Stop promotion; retry transient errors or quarantine the affected extract |

### 7.2 Required Decisions

- **Automatic Approval:** All critical checks pass, including connection, schema, required identifiers, primary-key uniqueness, extraction completeness, and required relationship checks.
- **Manual Review:** Non-critical warnings require review by the designated data-pipeline maintainer or administrator before promotion.
- **Rejection or Failure:** Critical failures stop handover. Transient connection or query failures may be retried up to the configured limit. If retries fail, the affected extract is held or quarantined and the issue is investigated before re-extraction.

### 7.3 Handover

The extraction stage passes the following to the transformation stage:

- Raw extracted datasets for the selected tables, stored under `rawdata/<table_name>/<run_date>/`.
- Extraction metadata, including run ID, timestamps, counts, and duration.
- Per-table validation results and any rejected-row or quarantine references.
- The extraction data contract and source schema notes.
- The processing period or grouping configuration, if one has been defined for the pipeline.

Example handover summary:

| Validation Summary | Result |
| --- | --- |
| `profiles` table validation | PASS |
| `rooms` table validation | PASS |
| `tenant_assignments` table validation | PASS |
| `bills` table validation | PASS |
| `tenant_requests` table validation | PASS |
| Overall Extraction | APPROVED |

> The example is illustrative only. Handover contains extracted and validated raw data. Data cleaning, standardization, aggregation, and business calculations are performed in the transformation stage.

---

## 8. Common Extraction Problems and Mitigation

| # | Problem description | Possible cause | Impact on extraction | Proposed solution / mitigation |
| --- | --- | --- | --- | --- |
| 1 | Supabase/PostgreSQL is unreachable | Network outage, incorrect credentials, or service maintenance | No records can be extracted and downstream stages cannot proceed | Retry temporary failures with Airflow; log the final error; verify credentials securely; stop handover if retries fail. |
| 2 | Required table or column is missing | Database schema change or mismatch between documentation and deployment | Extraction query fails or returns incomplete data | Check the schema before extraction; stop the affected task; confirm the change and update the contract before retrying. |
| 3 | Extracted record count differs from the source | Interrupted query, pagination issue, or concurrent source changes | Dataset may be incomplete | Compare counts for the same extraction scope; investigate the cause; re-extract before approval. |
| 4 | Duplicate primary keys appear | Incorrect pagination or repeated data during extraction | Duplicate records may distort downstream results | Validate key uniqueness; hold the affected dataset; fix extraction logic and re-run. |
| 5 | Required identifier is null | Incomplete source record or integrity issue | Record cannot be reliably identified or joined | Flag or quarantine the affected record; log the count; do not invent an identifier. |
| 6 | A relationship points to a missing record | Orphaned reference or related table is unavailable in the extract | Downstream joins may be incomplete | Verify the relationship and source data; document unresolved references and hold critical cases for review. |
| 7 | Large table extraction times out | Too many records fetched at once or query/network limits | Partial data or failed extraction | Use pagination or manageable batches; monitor duration; verify counts before marking the run successful. |
| 8 | Database credentials are exposed | Secrets hard-coded in code or printed in logs | Unauthorized database access may occur | Use protected Airflow connections or environment configuration; redact secrets; rotate exposed credentials. |
| 9 | An expected table returns no rows | Wrong query, permission issue, or genuinely empty source table | Missing data may be mistaken for a valid empty dataset | Compare with the source count and investigate unexpected empty results before approval. |
| 10 | Incremental extraction misses updates | A table has no reliable update timestamp or watermark | Changes to existing records may not reach the pipeline | Use full extraction until reliable change tracking is available; then implement and test watermark, overlap, and deduplication logic. |

---

## Expected Output

The extraction stage should provide:

1. Raw data from the selected Spartment operational tables.
2. Extraction run metadata, timestamps, counts, and status.
3. Extraction-level validation results.
4. A documented data contract and naming convention.
5. An acceptance decision and handover package for the transformation stage.

The README documents the intended pipeline. The actual extraction schedule, retry settings, staging location, incremental strategy, and log-retention policy must be aligned with the implemented configuration before deployment.
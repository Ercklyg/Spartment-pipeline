# Stage 3 Load Verification

This record distinguishes isolated implementation verification from deployment into the user's Supabase warehouse. The initial synthetic demonstration used no operational tenant data; later sections separately record the real-source extraction, validation and publication.

> **Current status:** the real-source batch was subsequently published successfully to the existing Supabase warehouse: **2,293 analytical rows**, with a verified pre-refresh backup, exact reconciliation, preserved prior batch history, and a proven duplicate no-op. The earlier synthetic results and holds below are historical evidence. See the final publication section for the actual production-target result. Scheduling remains inactive/unverified.

## Environment and Method

- Python 3.12.14 in the repository's `.venv`.
- psycopg 3.3.9 and PostgreSQL 18.1.
- Real temporary PostgreSQL instances bound to loopback, separate from the existing server on port 5432.
- The repeatable runner creates and removes its own disposable `stage3_tests` database cluster. Tests refuse database names outside `stage3_test*` before dropping test schemas.
- Existing Stage 2 fixtures generate the synthetic extraction handover and approved Stage 2 outputs. Load uses the resulting package rather than inventing a parallel analytical schema.

## Observed Synthetic Load

A retained demonstration was run in the separate `stage3_test_demo` database on the isolated test server. Evidence is stored locally at `loaded/demo/isolated-demonstration.json`; generated artifacts are excluded from Git.

| Operation | Observed result |
| --- | --- |
| Stage 2 synthetic transformation | APPROVED |
| Staging-only Load | DRY_RUN; no warehouse publication |
| Normal attempt with default disk-headroom policy | HELD; prior empty target unchanged |
| Rerun with recorded test-harness waiver of PRE-12 | SUCCESS; critical capacity reserve and all reconciliation checks still enforced |
| Repeating that same committed batch | SKIPPED_ALREADY_LOADED; zero additional analytical writes |
| Successful publication duration | 2,344 ms for this run; not a production performance guarantee |
| Total committed analytical records | 745 |

The PRE-12 warning concerns remaining free-space percentage after reservation. The mandatory measured PRE-08 capacity reserve passed. The waiver was an explicit **synthetic test-harness attestation**, not an approval to bypass warnings for actual operational data. This exercised warning hold, review, and revalidation rather than disabling the default policy.

| Table | Committed synthetic rows |
| --- | --- |
| `dim_date` | 730 |
| `dim_building` | 1 |
| `dim_room` | 2 |
| `dim_tenant` | 1 |
| `dim_lease` | 2 |
| `fact_assignment` | 1 |
| `fact_billing` | 2 |
| `fact_payment` | 1 |
| `fact_payment_correction` | 1 |
| `fact_inquiry` | 1 |
| `fact_tenant_request` | 1 |
| `fact_occupancy_event` | 1 |
| `fact_notification` | 1 |

| Financial reconciliation | Verified target total; exact incoming equality enforced |
| --- | --- |
| Billing `total_amount` | 16,700.00 |
| Billing `amount_paid` | 8,700.00 |
| Billing `balance` | 8,000.00 |
| Payment `amount` | 8,700.00 |

## Regression Evidence

Run the complete suite with:

```powershell
.\.venv\Scripts\python.exe tools/test_stage3.py --pg-bin "C:/Program Files/PostgreSQL/18/bin"
```

The database tests use actual PostgreSQL statements and assert:

- dry-run rollback, successful 13-table publication, and duplicate no-op;
- changed approved snapshot replacement without duplicate rows or inflated amounts;
- refusal of released but unapproved upstream input and corrupt files;
- refusal of stale reference dates and catalog/schema drift;
- exclusive advisory-lock behavior and insufficient-capacity rejection;
- rollback of the entire replacement after a forced mid-load SQL failure;
- rollback when a CHECK constraint fails or target values diverge from staged values;
- disk-headroom warning hold, exact-package review, and successful revalidation;
- restricted loader success and refusal of forbidden DDL/registry deletion;
- custom-format backup restoration into a separate empty database, recovering the previous published batch;
- local failure evidence when the target cannot be reached;
- committed-run recovery without another data load;
- type precision, boolean, integer overflow, text/null handling, and retention holds.

Operational tests exercise the retry count/delays, no retry for critical failure, stopping the daily chain on upstream rejection, and Airflow callback approval/wiring. Airflow imports are stubbed for those callback tests: they are **not** evidence of a running Airflow installation or an executed scheduled job.

The initial local-reference run finished with 43 tests passed. After adapting to the existing Supabase layout, the final complete run finished with **51 tests passed, zero failures, zero skips**, in **58.661 seconds**. This includes 21 existing Stage 2/retention tests, 17 local-reference Load tests, 8 existing-warehouse/configuration tests, and 5 operational tests. The disposable cluster was shut down and removed on exit.

## Deployment Work Still Requiring User Configuration

The initial local-server assumption was incorrect: the intended warehouse is hosted on Supabase. The `supabase_existing` adapter now uses its actual `warehouse_meta` design and exact types. Both the owner and provisioned restricted activity login passed live read-only catalog/count inspection. All 1,999 existing analytical rows remained unchanged. See [supabase-compatibility.md](supabase-compatibility.md).

The default runtime-role name already existed, so it was not reset. The distinct `spartment_activity_loader` login was provisioned and its generated credential saved only in `.env`. No warehouse DDL, refresh or history rewrite was performed. Replica tests demonstrate the write path; live inspection is not represented as a real-source Load.

The source Supabase API credentials were subsequently configured and verified against a different operational project. A real read-only extraction produced **1,134 rows across all 12 source tables**, with PASS validation and APPROVED acceptance. Its run ID is `spartment-extract-2026-11-13-0109`.

Transformation `spartment-transform-2026-11-13-0110` produced **2,293 analytical rows with zero rejected records**, but its decision was MANUAL REVIEW. V-18 flagged four optional fields above its 90% null-rate rule:

| Field | Observed null rate |
| --- | --- |
| dim_tenant.lease_end | 93.75% |
| dim_lease.lease_end_date | 93.10% |
| fact_assignment.lease_end | 93.10% |
| fact_inquiry.room_key | 100% |

These fields permit legitimate nulls in the contract, including open-ended leases and inquiries without a selected room. This observation does not automatically override Stage 2's profiling policy. No upstream rule, source row, handover decision or approval gate was changed to force a load.

The actual new adapter was invoked with that real handover in dry-run mode. It refused before warehouse staging/publication with `UPSTREAM_APPROVAL`, `published=false`. The local attempt ID is `spartment-load-2026-11-13-011044-70324f04`. A subsequent restricted-role read-only inspection confirmed the same 1,999 live warehouse rows. This verifies the hold boundary, not a successful real-source publication.

Next operational step: resolve/review the Stage 2 profiling policy and obtain a genuinely APPROVED handover, then satisfy the hosted capacity/review gates. An explicit Stage 2 manual-release flag alone remains insufficient for this Load contract.

## V-18 Correction and Approved Real-Batch Dry Run

The V-18 implementation treated permitted open-ended dates and unselected inquiry rooms as quality failures solely because their null rate exceeded 90%. Rules version 1.1 and optional-null profile version 2 now record raw/expected null counts separately and monitor the unexpected-null rate among eligible rows. Conditional ended-assignment and OTHER-request room absences are also recognized. Contact/reference-note quality checks remain active. Old raw-rate logs are not compared as if they used the new eligible population.

Four integration regressions verify that legitimate open-ended/unselected values pass, high missing-contact rates still hold a batch, contact degradation still warns, and old profiling baselines are not silently mixed. The full suite passed **55 tests, zero failures/skips**, in **56.586 seconds**.

Reprocessing the same approved extraction produced `spartment-transform-2026-11-13-0118`: **APPROVED**, 2,293 output rows. No source values, cleaning rules or business calculations were changed, and no manual approval flag was used.

Supabase dry run `spartment-load-2026-11-13-011902-593564f2` successfully staged all 13 tables (2,293 rows), then rolled back without publishing. Duration: 45,953 ms. The observed table counts staged were 1,159 dates, 4 buildings, 33 rooms, 32 tenants, 29 leases, 29 assignments, 279 bills, 210 payments, 3 corrections, 1 inquiry, 5 requests, 1 occupancy event and 508 notifications.

PRE-08 passed: database usage 15,472,307 bytes, computed reserve 26,371,072 bytes, configured budget 450,000,000 bytes; local work/backup space also sufficient. PRE-12 warned because no fresh project-bound physical-disk observation was supplied. The user explicitly selected keeping the existing snapshot pending disk evidence; no warning waiver was approved. The prior 1,999-row warehouse snapshot remains published, confirmed by a subsequent read-only inspection.

All 13 analytical content-checksum values before and after the V-18 correction matched. Only the validation/profile behavior and transformation run lineage changed. A blank ignored `loaded/capacity.json` template was prepared with the correct destination project reference and null observation/headroom fields; it cannot pass the capacity-evidence check until populated with a real observation.

## User-Supplied Disk Evidence and Repeat Dry Run

The user supplied an Infrastructure screenshot for the warehouse showing 16% disk usage: database 27.6 MB, WAL 112 MB, system 167.6 MB. These displayed components total 307.2 MB. A conservative available-space estimate of 1,400,000,000 bytes was recorded, with its derivation and screenshot provenance in the ignored capacity file. This is an inferred lower bound from rounded dashboard values, not an exact filesystem measurement or an assumed total disk capacity.

Repeat dry run `spartment-load-2026-11-13-012853-b0d0c230` staged all 2,293 rows with no critical failures and no publication. Fresh project-bound server evidence was accepted. PRE-12 remained WARNING solely because the laptop's roughly 30.7 GB free space represents about 12% of its drive, below the configured 20% warning percentage, although the approximately 109 MB critical local reserve passed.

Automatic approval review rejected an attempted publication because the user's earlier instruction retained the current snapshot and the screenshot alone did not clearly reauthorize replacement. That rejected command did not run. Explicit user approval to accept the remaining local-disk warning and replace the snapshot was requested after the completed dry run. No approval is inferred from the screenshot itself.

## Successful Real-Source Publication

After the explicit question describing replacement counts, backup, and the local percentage warning, the user delegated the decision with “do whats good.” The assistant recorded a batch/checksum-scoped review accepting PRE-12 only: approximately 30.7 GB available locally versus 109 MB required, and conservative screenshot-derived Supabase headroom of 1.4 GB versus about 26.4 MB required. No critical check or upstream approval was waived.

Load `spartment-load-2026-11-13-013204-1570591f` completed **SUCCESS / APPROVED / published=true**, replacing the previous snapshot with **2,293 real-source analytical rows**. Duration: **113,562 ms**. All critical checks and exact staging/target value/count/money reconciliation passed before commit. The recorded non-critical warning remained visible with its review disposition.

The previous snapshot was downloaded to an ignored custom-format dump before replacement. The archive readability check passed, and an independent post-load SHA-256 verification matched the manifest. Archive size: **150,859 bytes**. This individual archive was checked for readability/integrity; restoration behavior was demonstrated separately in disposable integration databases, not by overwriting the live target.

| Target | Committed rows |
| --- | --- |
| dim_date | 1,159 |
| dim_building | 4 |
| dim_room | 33 |
| dim_tenant | 32 |
| dim_lease | 29 |
| fact_assignment | 29 |
| fact_billing | 279 |
| fact_payment | 210 |
| fact_payment_correction | 3 |
| fact_inquiry | 1 |
| fact_tenant_request | 5 |
| fact_occupancy_event | 1 |
| fact_notification | 508 |
| **Total** | **2,293** |

Selected exact financial reconciliation: payment amount **341,036.00 incoming = 341,036.00 target**; billing total **496,396.00 incoming = 496,396.00 target**. Every other money column was also checked by the loader, along with a full analytical multiset comparison for every table.

Independent restricted-role read-only inspection confirmed all 13 committed counts. A final registry check confirmed the current pointer and file-checksum map match this run and the prior batch history remains present.

Replaying the exact package created attempt `spartment-load-2026-11-13-013529-523b8309` with **SKIPPED_ALREADY_LOADED / APPROVED / published=false**. It performed zero additional analytical writes and retained the successful publication as current. A no-op attempt's published=false means that attempt published nothing new, not that the existing data is unavailable.

Real manual execution is verified. Neither scheduler was activated or represented as demonstrated by this publication. Code changes remain uncommitted unless a later user action commits them.

Neither the optional Airflow schedule nor a Windows scheduled task is claimed as active. The Task Scheduler registration script creates a disabled task by default for review. A real scheduled run must be observed after valid credentials and a successful manual pipeline execution. The laptop must remain on and awake while the local pipeline runs.

The implementation, schema DDL, nine-section load document, test runner, and synthetic demonstration support the activity. A submission claiming real-source deployment or active daily scheduling must add that deployment's actual evidence.

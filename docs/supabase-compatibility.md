# Supabase Destination Compatibility Review

## Connection Verification

The owner connection configured privately in `.env` successfully reached the Supabase destination. The actual database name is `postgres`, and its reported PostgreSQL version is 17.11. The Supabase project name must not be substituted for the database name in the DSN.

The inspection used read-only metadata and count queries. It did not create schemas, reset role passwords, change authentication, or publish analytical records. No password or full connection string is recorded here.

## Existing Destination

The destination already contains all 13 analytical tables in `warehouse`, an existing `spartment_loader` role, and these control objects:

| Object | Existing responsibility |
| --- | --- |
| `warehouse_meta.attempts` | Attempt ID, batch, status, recorded timestamp, JSON report |
| `warehouse_meta.batches` | Batch and transformation/extraction IDs, processing period, snapshot end, fingerprint, completed run/time, JSON report |
| `warehouse_meta.current_batch` | Singleton pointer to a batch |
| `warehouse_meta.schema_contract` | Version, fingerprint, and JSON catalog |

All warehouse tables reference `warehouse_meta.batches` through `load_batch_id`, using deferred batch foreign keys.

| Table | Rows observed during inspection |
| --- | --- |
| `dim_date` | 1,159 |
| `dim_building` | 3 |
| `dim_room` | 30 |
| `dim_tenant` | 30 |
| `dim_lease` | 27 |
| `fact_assignment` | 27 |
| `fact_billing` | 217 |
| `fact_payment` | 144 |
| `fact_payment_correction` | 0 |
| `fact_inquiry` | 1 |
| `fact_tenant_request` | 0 |
| `fact_occupancy_event` | 0 |
| `fact_notification` | 361 |
| **Total** | **1,999** |

These counts are observations of pre-existing data, not evidence of a load performed by the new implementation. Tenant values were not printed or copied into this review.

## Initial Differences and Adapter Resolution

| Area | Existing Supabase destination | Adapter behavior |
| --- | --- | --- |
| Database | `postgres` | Accepts TLS direct/session Supabase connections; rejects transaction pooling |
| Control schema | `warehouse_meta` | Uses existing attempts, batches, current_batch and schema_contract; no load_control creation |
| Row lineage | `load_batch_id`; existing upstream run ID where applicable | Inserts only existing lineage columns; run/time resolved through metadata |
| Keys | Fixed-length character keys | Preserves existing values and validates exact lengths |
| Other types | Smallint, bounded varchar/numeric | Validates actual widths/ranges/scales before COPY; no new rounding |
| Snapshot identity | Existing batch fingerprint, snapshot timestamp, deferred FK | New reports add versioned checksums; ambiguous legacy replay is refused; deferred FKs checked before commit |
| Storage checks | Hosted physical disk unavailable through local paths | Measures DB size and budget; missing fresh dashboard headroom produces a warning hold |
| Roles | Existing spartment_loader and spartment_pipeline_loader | Preserved; new restricted spartment_activity_loader provisioned with saved private runtime DSN |

## Verification and Publication Boundary

Both owner and new runtime connections passed read-only live inspection, including the strict catalog check. The table counts remained identical to the 1,999-row baseline. Provisioning changed only the new login/grants; it did not reset existing roles or modify analytical tables, registry history or rows.

The user selected adaptation to the existing design. Eight focused tests passed against a disposable schema replica; the full 51-test regression also passed. These include successful publication/no-op, legacy-evidence refusal, target-width enforcement, hosted capacity hold, restricted-role permissions and preservation of a prior snapshot/history after forced mid-load failure.

No operational refresh was performed during adaptation. Real extraction was approved, but the new transformation batch was held by V-18 optional-field null profiling; the adapter correctly rejected that handover with UPSTREAM_APPROVAL before staging. The same 1,999 destination rows were confirmed afterward. A genuinely APPROVED transformation and the hosted capacity/review gates are still required before publication. The local-reference `setup` command explicitly refuses Supabase targets.

### Subsequent Validated Publication

V-18 was corrected to distinguish contract-permitted absence from unexpected missing data, with regression coverage and rules version 1.1. The resulting real-source batch became APPROVED without altering analytical values. After the user supplied disk evidence and delegated the remaining local-warning decision, the scoped review, backup, reconciliation and atomic publication completed successfully. The warehouse now contains **2,293 rows**. The old batch history and backup were preserved, and replay was verified as a zero-write no-op. See [load-verification.md](load-verification.md) for exact run IDs, counts and evidence; the original 1,999-row table above remains an adaptation baseline, not the current snapshot.

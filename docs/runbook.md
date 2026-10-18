# Runbook

Day to day operation of the Spartment pipeline.

## Daily run

The Airflow DAG `spartment_daily_extraction` runs at 01:00 Asia/Manila with up to 3 retries. Nothing is required manually on a normal day.

To run it by hand:

```bash
spartment-extract
```

## Reading the result

```
acceptance    APPROVED          promote to transformation
acceptance    MANUAL REVIEW     promotion held, exit 5
acceptance    REJECTED          promotion blocked, exit 4
```

The handover record is the entry point for every investigation:

```
rawdata/_handover/<run_id>.json
```

It contains the acceptance decision and its rationale, the responsible party, per table validation summary, staged dataset paths, extraction metadata, all validation checks with evidence, rejected and quarantined references, the standardized column mapping, the column type manifest, and the processing period.

## Exit codes

| Code | Meaning | Action |
| --- | --- | --- |
| 0 | `APPROVED` | None |
| 1 | Extraction failure | Investigate the error code in the run record, re-run |
| 2 | Configuration or argument error | Check `.env` exists at the repository root |
| 3 | Schema unreachable | Confirm network and credentials, re-run |
| 4 | `REJECTED` | See "Rejection" below |
| 5 | `MANUAL REVIEW` | Review the listed warnings, then record a decision |

## Rejection

A `REJECTED` run has a critical failure. Do not promote.

| Cause | Where to look | Recovery |
| --- | --- | --- |
| Connectivity lost | `source_connection_accessibility` evidence | Confirm the endpoint and credentials, re-run |
| Schema drift | `required_tables_and_columns_exist` evidence | Compare the live schema with `contract.py`, confirm the change, update the contract, re-extract |
| Duplicate primary keys | `primary_key_uniqueness` evidence | Indicates a pagination defect or concurrent source writes. Re-extract; if it repeats, stop and investigate |
| Incomplete extraction | `record_count_completeness` or `extraction_errors_or_interruptions` | Re-run the affected tables |
| Null required identifier | `required_identifiers_present` evidence | Flagged rows are listed with their CSV line number. Do not invent identifiers |

Escalate to the Database Owner when the defect is on the source side.

## Manual review

Non critical warnings hold promotion without failing the run. Common causes are negative monetary values, inverted dates, unresolved relationship references, room sizes outside the permitted range, and assignment status inconsistent with the room reference.

Review the evidence in the handover record, then either correct the source data and re-run, or record a decision to promote with the warning accepted.

## Re-extracting a single table

```bash
spartment-extract --tables rooms
```

## Recovery

Every run is independent and full. To rebuild, re-run the affected tables. Raw datasets are reproducible from the source, so no backup of `rawdata/` is required.

Both the failed run and the recovery run remain traceable by run id. Run ids are `spartment-extract-<date>-<HHMM>` with a counter suffix when a minute is reused, so nothing is overwritten.

## Retention

| Data | Period |
| --- | --- |
| Staged datasets and per table run metadata | 90 days |
| Validation and handover records | 365 days |

Configured in `config/extraction.toml`.

```bash
spartment-retain          # dry run, lists what would be removed
spartment-retain --apply  # perform deletions
```

The dry run is always safe. `--apply` is required before anything is deleted, and nothing inside its retention window is ever removed. Audit records survive operational pruning.

## Airflow

The DAG needs the package installed in the Airflow environment:

```bash
pip install -e .
```

Airflow places only its own `dags_folder` on `sys.path`, so a repository relative import would not resolve. The DAG reads credentials from the Airflow connection `spartment_postgres`.

Point the scheduler at the repository:

```bash
airflow dags list          # with --dags-folder configured
```

`max_active_runs=1`, so overlapping runs cannot occur. The gate task fails the run on `REJECTED` and logs a warning on `MANUAL REVIEW`.

## Credentials

`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are read from `.env` at the repository root. The file is excluded from version control. See `.env.example`.

The anon key is not used. The database superuser password is never required and must never be placed in this repository.

If the service role key is ever committed or pasted into a channel, rotate it in Supabase under Project Settings, then API.

## Known limitations

- `auth.users` is not reachable, so `tenant_assignments.id` and `payment_corrections.corrected_by` cannot be referentially validated.
- Enum members are not exposed by PostgREST, so domains are reported as observed rather than asserted.
- Pages are ordered by the immutable `id`, so nothing is skipped or duplicated, but a run is not a single consistent database instant.
- `rooms.updated_at` is populated in practice but has no trigger in `migrations/`, so it is not trusted as a watermark.
- The DAG has not been executed in this environment; it is syntax checked only.
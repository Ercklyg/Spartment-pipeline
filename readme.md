# Spartment Pipeline

Operational and analytical batch data pipeline for the Spartment apartment management system.

Stage 1 extracts the operational tables from Supabase into `rawdata/`, validates them, and records an acceptance decision. Stage 2 transforms one validated Stage 1 batch into the analytical model under `transformed/`. Stage 3 (load) is reserved.

## Documentation

- [Stage 1: Data Extraction](docs/extraction.md) - extraction specification, data contract, and operations runbook
- [Stage 2: Data Transformation](docs/transformation.md) - transformation specification and output contract

## Layout

```
config/extraction.toml        non secret settings
dags/                         Airflow DAGs
src/spartment/
  common/                     config, models, JSON writers, retention
  stage1_extract/             contract, source, guard, extract, validate, handover, cli
  stage2_transform/           cleaning, dimensions, facts, validation, handover, cli
  stage3_load/                reserved
migrations/                   SQL migrations for the source database
docs/                         stage specifications
rawdata/                      staged output, not version controlled
transformed/                  transformed output, not version controlled
logs/                         run logs, not version controlled
quarantine/                   rejected records, not version controlled
```

Stages are independent packages. Stage 2 reads `rawdata/` and imports `spartment.common.models`; it never imports Stage 1 internals and never connects to Supabase.

## Setup

```bash
pip install -e .
cp .env.example .env      # then fill in SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY
```

The `.env` file lives at the repository root and is excluded from version control. A database superuser password is never required.

If the console scripts are not on your PATH, use the module form:

```bash
python -m spartment.stage1_extract.cli
python -m spartment.stage2_transform.cli
python -m spartment.stage2_transform.retention
python -m spartment.common.retention
```

## Commands

```bash
spartment-extract                        # extract, validate, gate
spartment-extract --tables rooms,bills   # subset of tables
spartment-extract --strict-contract      # reject on any schema drift
spartment-extract --no-extra-columns     # contract columns only
spartment-extract --skip-validation      # debugging only

spartment-transform                      # transform the current Stage 1 batch
spartment-transform --parent-run-id ID   # transform a specific Stage 1 run
spartment-transform --allow-manual-review  # release a held MANUAL REVIEW batch
spartment-transform --dry-run            # validate without writing files

spartment-retain                         # Stage 1 retention dry run, nothing removed
spartment-retain --apply                 # perform Stage 1 deletions

spartment-transform-retain               # Stage 2 retention dry run, nothing removed
spartment-transform-retain --apply       # archive/prune expired Stage 2 logs
spartment-transform-retain --apply --include-quarantine  # also remove expired quarantine
```

Stage 2 per-table logs expire after `retention.operational_days` (default 90)
and are gzip-compressed into `logs/spartment-transformation/_archive/` when
`--apply` is used. Run summaries expire after `retention.audit_days`
(default 365) and are archived on the same schedule. Rejected-record
quarantine files also expire after the audit window, but their removal is
opt-in via `--include-quarantine`. Deletion/archival writes an audit record
under `logs/spartment-transformation/_retention/`.

### Exit codes

Stage 1:

| Code | Meaning |
| --- | --- |
| 0 | `APPROVED` |
| 1 | Extraction failure |
| 2 | Configuration or argument error |
| 3 | Schema unreachable |
| 4 | `REJECTED` |
| 5 | `MANUAL REVIEW` |

Stage 2:

| Code | Meaning |
| --- | --- |
| 0 | `APPROVED`, or released `MANUAL REVIEW` |
| 1 | Rejected or transformation failure |
| 2 | Configuration or argument error |
| 3 | Unusable parent, input error, or schema drift |
| 5 | `MANUAL REVIEW` held |

## Output

Stage 1:

```
rawdata/<table>/<run_date>/<table>.csv   staged dataset
rawdata/_metadata/<run_id>.json          per table run record
rawdata/_validation/<run_id>.json        validation checks
rawdata/_handover/<run_id>.json          acceptance decision and handover package
```

Stage 2:

```
transformed/<table>/<run_date>/<table>_<run_date>.csv  transformed dataset
transformed/_validation/<run_id>.json                  validation checks and decision
transformed/_handover/<run_id>.json                    transformed handover package
logs/spartment-transformation/<run_date>/<run_id>_<table>.json  per table run log
logs/spartment-transformation/<run_date>/<run_id>.json          run summary
quarantine/transformation/<run_date>/<table>_rejected.csv       rejected records
```

Run ids are `spartment-extract-<date>-<HHMM>` and `spartment-transform-<date>-<HHMM>`, with a counter suffix if the same minute is reused, so run logs are not overwritten. Transformed datasets are intentionally idempotent by `run_date`.

## Schedule

The Airflow DAG `spartment_daily_extraction` runs at 01:00 Asia/Manila daily with 3 retries and exponential backoff. Tasks: `extract_and_validate` -> `acceptance_gate` -> `retain`. Credentials come from the Airflow connection `spartment_postgres`.

The Airflow DAG `spartment_daily_transformation` runs at 02:00 Asia/Manila daily. Tasks: `transform_and_validate` -> `acceptance_gate` -> `retain`. Only transformation read/parse failures retry (3 retries, 5-minute exponential backoff); schema/data defects fail immediately. The acceptance gate runs after a successful transformation, resolves the parent Stage 1 run from the DAG logical date, holds promotion when the decision is `MANUAL REVIEW`, and fails the DAG for rejected batches. The retention task runs regardless of upstream failure.

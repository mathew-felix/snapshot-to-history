# Build Tracker

## Current Phase

Phase 4 - dbt late-arrival repair and reproducible pipeline run.

## Completed

- Read the project blueprint from `plan.md`.
- Confirmed the existing repo already has the base Python/PostgreSQL/dbt SCD2 pipeline.
- Started a clean implementation tracker for the Airflow and failure-recovery buildout.
- Added `dags/business_registry_scd2_daily.py` with daily schedule, catchup, retries, exponential backoff, logical-date passing, sequential dbt tasks, and artifact archiving.
- Added `src/archive_dbt_artifacts.py` for preserving dbt observability files under `docs/run_artifacts/`.
- Added `--snapshot-date` support to `src.extract`.
- Added raw load audit, DLQ, schema drift event, and pipeline metric tables.
- Added schema profiling and DLQ routing to `src.load_raw`.
- Reworked raw loading to use a transaction-scoped staging table before replacing the target `load_date`.
- Added advisory lock protection for the critical section keyed by `business_registry_scd2:<load_date>`.
- Added tests for DLQ routing and schema drift logging.
- Verified Docker test suite: `7 passed`.
- Added `public_marts.dim_business`, a deterministic SCD2 table rebuilt from all staged snapshots.
- Added `public_staging.scd2_repair_keys` as an operational signal for late-arriving/backfill repair.
- Redirected marts, summary output, and invariant tests to the repaired SCD2 dimension.
- Added a reusable SCD2 repair macro.
- Added `.gitattributes` and pinned `protobuf<5` to keep dbt 1.7.9 reproducible in Docker.
- Verified full container pipeline: dbt built 6 models, passed 13 tests, and printed a 2,000-row run summary.

## In Progress

- Commit dbt late-arrival repair and reproducible pipeline run.

## Next

- Phase 5: Add chaos fixtures/tests and verify the pipeline.
- Phase 6: Refresh diagrams and recruiter-optimized README.

# Build Tracker

## Current Phase

Phase 3 - Transactional ingestion hardening.

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

## In Progress

- Commit transactional ingestion hardening.

## Next

- Phase 4: Add dbt models/tests for observability and late-arrival repair.
- Phase 5: Add chaos fixtures/tests and verify the pipeline.
- Phase 6: Refresh diagrams and recruiter-optimized README.

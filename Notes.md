# Build Tracker

## Current Phase

Phase 2 - Audit/DLQ/schema drift foundation.

## Completed

- Read the project blueprint from `plan.md`.
- Confirmed the existing repo already has the base Python/PostgreSQL/dbt SCD2 pipeline.
- Started a clean implementation tracker for the Airflow and failure-recovery buildout.
- Added `dags/business_registry_scd2_daily.py` with daily schedule, catchup, retries, exponential backoff, logical-date passing, sequential dbt tasks, and artifact archiving.
- Added `src/archive_dbt_artifacts.py` for preserving dbt observability files under `docs/run_artifacts/`.
- Added `--snapshot-date` support to `src.extract`.
- Added raw load audit, DLQ, schema drift event, and pipeline metric tables.
- Added schema profiling and DLQ routing to `src.load_raw`.

## In Progress

- Run targeted checks and commit Airflow plus audit/DLQ foundation.

## Next

- Phase 3: Harden raw loading for idempotency, transactional staging, and schema drift.
- Phase 4: Add dbt models/tests for observability and late-arrival repair.
- Phase 5: Add chaos fixtures/tests and verify the pipeline.
- Phase 6: Refresh diagrams and recruiter-optimized README.

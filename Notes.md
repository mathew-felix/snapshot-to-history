# Build Tracker

## Current Phase

Phase 6 - Portfolio README and generated diagrams.

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
- Added `tests/test_chaos_recovery.py`.
- Verified required schema drift fails before raw mutation while preserving a `staging.schema_drift_events` record.
- Verified late-arriving records rebuild SCD2 windows in chronological order.
- Verified a simulated post-swap checksum failure rolls back the raw partition to its previous committed state.
- Verified Docker test suite: `10 passed`.
- Reran the clean pipeline after chaos tests; dbt passed 13 tests and summary returned to the 2,000-row sample state.
- Generated refreshed architecture and before/after diagrams and saved them to `docs/`.
- Rewrote `README.md` as a recruiter-facing project page with badges, visuals, architecture, data quality gates, Airflow DAG details, warehouse map, and interview talking points.
- Final verification passed: `docker-compose run --rm runner pytest -q tests/` returned `10 passed`; `docker-compose run --rm runner bash run.sh` built dbt successfully, passed 13 dbt tests, and printed the 2,000-row run summary.

## In Progress

- Commit portfolio assets and finish handoff.

## Next

- Final: confirm git status and hand off.

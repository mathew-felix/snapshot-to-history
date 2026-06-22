# Snapshot to History: SCD Type 2 Business Registry Warehouse

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt_Core-1.7-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-DAG-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-Reproducible-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-23_passing-brightgreen)

**A batch warehouse that turns overwrite-only business-license snapshots into queryable point-in-time history.**

NYC Open Data publishes a current-state business license feed. A naive pipeline overwrites yesterday's values and permanently loses prior addresses, statuses, and license attributes. This project loads snapshots into PostgreSQL, normalizes dirty source values with dbt Core, builds an SCD Type 2 dimension, and includes recovery paths for schema drift, late-arriving records, and mid-load failures.

![Before vs After SCD2](docs/before_after_scd2.svg)

## Quickstart

```bash
git clone https://github.com/mathew-felix/snapshot-to-history.git
cd snapshot-to-history
cp .env.example .env
make run
```

`make run` starts PostgreSQL, loads the committed sample snapshot, runs dbt snapshot/run/test, and prints the warehouse summary.

```text
=========== SCD2 BUSINESS REGISTRY - RUN SUMMARY ===========
Snapshot date             : 2026-06-27
Raw rows ingested         : 2,000
Total versions in history : 2,000
Current rows (is_current) : 2,000
Unique active businesses  : 2,000
Current row check (direct): 2,000
=============================================================
```

## Architecture

![Pipeline Architecture](docs/pipeline_architecture.svg)

```text
NYC Open Data API
  -> Airflow DAG logical date and retry policy
  -> raw.businesses_snapshot plus audit and DLQ tables
  -> dbt staging normalization, casting, dedupe, attr_hash
  -> public_marts.dim_business repaired SCD2 ranges
  -> analytical marts and invariant tests
```

The Airflow DAG in `dags/business_registry_scd2_daily.py` is configured for daily execution with `catchup=True`, `max_active_runs=1`, retries, exponential backoff, and `{{ ds }}` passed into every extract/load/dbt command. That makes normal daily runs and historical backfills use the same deterministic batch key: `load_date`.

## Engineering Decisions

**Idempotent partition replacement**

The raw loader takes an advisory lock for `business_registry_scd2:<load_date>`, stages rows in a transaction-scoped table, validates staged counts, deletes only the target `load_date`, and swaps the new rows in one transaction. Re-running the same date replaces the same partition instead of appending duplicates.

**Schema drift is observable**

Incoming headers are compared against the expected source contract. Missing required natural-key fields fail fast and persist a row in `staging.schema_drift_events`. Bad records with missing `license_nbr` are routed to `raw.businesses_snapshot_dlq` with reason codes instead of silently disappearing.

**Late-arriving records are repaired**

The final `public_marts.dim_business` table is rebuilt from all staged snapshots ordered by `load_date`. That avoids trusting dbt snapshot arrival order when a backfill lands after newer data. The model recomputes `valid_from`, `valid_to`, and `is_current` from chronological change points.

**Failure recovery is tested**

The test suite intentionally simulates:

- Required upstream schema drift before raw mutation.
- Backdated records arriving after newer snapshots.
- A forced post-swap checksum failure to verify PostgreSQL rollback preserves the previous committed partition.

## Data Quality Gates

```bash
make test
```

Coverage includes:

- `10` Python tests for extraction, idempotent loading, DLQ behavior, schema drift, rollback, and late-arrival repair.
- `13` dbt tests for source not-null checks, SCD2 uniqueness, one-current-row guarantees, and no overlapping validity windows.

Core SCD2 invariants:

- `assert_one_current_per_key`: no `license_nbr` has more than one current row.
- `assert_no_overlapping_ranges`: no two versions for the same business overlap in effective time.

## Warehouse Tables

| Layer | Object | Purpose |
|---|---|---|
| Bronze | `raw.businesses_snapshot` | Full source snapshot rows stored as text plus `load_date` and `ingested_at`. |
| Bronze | `raw.businesses_snapshot_load_audit` | One audit record per logical load date with row counts and checksum. |
| Bronze | `raw.businesses_snapshot_dlq` | Rejected records with source row number and reject reason. |
| Silver | `public_staging.stg_businesses` | Clean typed rows, normalized tracked attributes, deterministic `attr_hash`. |
| Silver | `public_staging.scd2_repair_keys` | Operational signal for keys affected by late-arriving history. |
| Gold | `public_marts.dim_business` | Repaired SCD Type 2 dimension with `valid_from`, `valid_to`, `is_current`. |
| Gold | `public_marts.vw_address_changes` | Address-change analytics view. |
| Gold | `public_marts.vw_status_history` | License status history view. |
| Gold | `public_marts.quality_summary` | Run summary consumed by `src.summary`. |

## Airflow DAG

The DAG task graph is:

```text
validate_runtime_config
  -> extract_snapshot_to_local
  -> profile_source_schema
  -> load_raw_replace_partition
  -> dbt_deps_compile
  -> dbt_source_freshness
  -> dbt_run_staging
  -> dbt_snapshot_scd2
  -> dbt_run_gold_marts
  -> dbt_test_invariants
  -> publish_run_summary
  -> archive_dbt_artifacts
```

Airflow is not required for the local `make run` path. The DAG is included for scheduled execution in an Airflow environment where this repo is mounted on a worker image with the same Python/dbt dependencies.

## Repository Map

```text
dags/                         Airflow DAG
src/extract.py                 Socrata extract with stable pagination
src/load_raw.py                Transactional raw loader with DLQ/audit/schema profiling
src/archive_dbt_artifacts.py   dbt artifact archiver
sql/init/                      PostgreSQL schemas, raw, DLQ, audit, metrics tables
dbt/models/staging/            Clean typed source models and repair-key detection
dbt/models/marts/              SCD2 dimension and analytics marts
dbt/tests/                     SCD2 invariant tests
tests/                         Python and chaos recovery tests
docs/                          Architecture and SCD2 comparison diagrams
plan.md                        Build blueprint and failure-recovery strategy
```

## Technical Highlights

- Airflow logical date is the batch key, so retries and backfills mutate the same `load_date` deterministically.
- PostgreSQL transactions and advisory locks make raw partition replacement safe under task retries and mid-load failures.
- Schema drift is logged, unkeyable rows are quarantined, and dbt tests stop invalid history from reaching the marts.
- Late-arriving records are handled by rebuilding effective ranges from ordered staged history instead of trusting arrival order.

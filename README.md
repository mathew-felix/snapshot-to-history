# Snapshot to History: SCD Type 2 Business Registry Warehouse

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt_Core-1.7-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Apache_Airflow-DAG-017CEE?logo=apacheairflow&logoColor=white)
![Docker](https://img.shields.io/badge/Docker_Compose-Reproducible-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-23_passing-brightgreen)

**A reproducible batch data warehouse that preserves historical business-license state from overwrite-only public snapshots.**

NYC Open Data publishes a current-state business license feed. A naive load overwrites yesterday's values and permanently loses prior addresses, statuses, and license attributes. This project models those snapshots as SCD Type 2 history using Python, PostgreSQL, dbt Core, Docker Compose, and an authored Airflow DAG design. The implementation includes idempotent raw loads, transactional partition replacement, audit tables, DLQ routing, schema drift detection, dbt transformations, and point-in-time SCD2 effective dating.

![Before vs After SCD2](docs/before_after_scd2.svg)

## Quickstart

```bash
git clone https://github.com/mathew-felix/snapshot-to-history.git
cd snapshot-to-history
cp .env.example .env
make run
```

`make run` starts PostgreSQL, loads the committed 2,000-row sample snapshot, runs dbt snapshot/run/test, and prints the warehouse summary.

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

## Local Demo vs Live Scale

`make run` uses a committed 2,000-row sample so reviewers can run the full warehouse locally without API credentials, network dependency, or long runtimes.

The pipeline is designed around a live extraction path from NYC Open Data. The same extract/load/dbt flow can run against larger source snapshots by changing the extraction configuration.

The 2,000-row sample is a reproducibility artifact, not the architectural limit of the pipeline.

| Run Mode | Source | Rows Loaded | Purpose |
|---|---|---:|---|
| Local reviewer run | Committed sample snapshot | 2,000 | Fast reproducible demo |
| Live API run | NYC Open Data API | Pending benchmark | Full source extraction benchmark |
| Backfill run | Multiple logical dates | Pending benchmark | Historical SCD2 repair validation |

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

## Operational Guarantees

| Guarantee | Mechanism | Artifact |
|---|---|---|
| Retry for the same date does not duplicate raw rows | Advisory lock plus delete/insert inside one PostgreSQL transaction | `src/load_raw.py` |
| Bad records do not silently disappear | Missing `license_nbr` rows are written with reason codes | `raw.businesses_snapshot_dlq` |
| Source schema drift is queryable after failure | Header profile is persisted before required-column failures abort the load | `staging.schema_drift_events` |
| Backfills do not corrupt current rows | `dim_business` derives ranges from all staged snapshots ordered by `load_date` | `dbt/models/marts/dim_business.sql` |
| Invalid SCD2 state fails the build | dbt singular tests enforce one current row and no overlaps | `dbt/tests/` |

Raw partition replacement follows this transaction shape:

```sql
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('business_registry_scd2:' || :load_date));
CREATE TEMP TABLE businesses_snapshot_stage (...);
-- insert rows, route rejects, validate staged count
DELETE FROM raw.businesses_snapshot WHERE load_date = :load_date;
INSERT INTO raw.businesses_snapshot SELECT * FROM businesses_snapshot_stage;
COMMIT;
```

## Failure Modes Tested

| Incident | Failure injected | Expected recovery |
|---|---|---|
| Required source column missing | CSV missing required header | Loader fails before raw mutation; schema drift event remains queryable |
| Unkeyable record | Blank `license_nbr` | Row lands in `raw.businesses_snapshot_dlq`; valid rows continue loading |
| Late-arriving snapshot | Older `load_date` loaded after a newer date | `dim_business` recomputes non-overlapping SCD2 ranges |
| Mid-load failure | Exception before transaction commit | Previous committed `load_date` partition remains intact |

## Post-Mortem Evidence

Each simulated failure is expected to leave evidence in a specific place:

| Evidence | Where to look |
|---|---|
| Orchestration failure | Airflow task logs or local command output |
| Bad input row | `raw.businesses_snapshot_dlq` |
| Source contract issue | `staging.schema_drift_events` |
| Historical corruption risk | `assert_one_current_per_key` and `assert_no_overlapping_ranges` |

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

## Benchmarks

Benchmarks should be regenerated after each major pipeline change. Pending values are intentionally left blank until measured from actual runs.

| Scenario | Rows | Runtime | Status |
|---|---:|---:|---|
| Local sample run | 2,000 | Pending benchmark | Reproducible with `make run` |
| Live API run | Pending benchmark | Pending benchmark | Run with `make live` |
| Same-date retry | 2,000 | Pending benchmark | Confirms idempotent partition replacement |
| Late-arriving snapshot repair | Pending benchmark | Pending benchmark | Confirms SCD2 range rebuild |

## Warehouse State

| Layer | Object | Purpose |
|---|---|---|
| Bronze | `raw.businesses_snapshot` | Full source snapshot rows stored as text plus `load_date` and `ingested_at`. |
| Bronze | `raw.businesses_snapshot_dlq` | Rejected records with source row number and reject reason. |
| Bronze | `raw.businesses_snapshot_load_audit` | One audit record per logical load date with row counts and checksum. |
| Silver | `public_staging.stg_businesses` | Clean typed rows, normalized tracked attributes, deterministic `attr_hash`. |
| Gold | `public_marts.dim_business` | Repaired SCD Type 2 dimension with `valid_from`, `valid_to`, `is_current`. |

## Airflow DAG

The Airflow DAG in `dags/business_registry_scd2_daily.py` is authored for scheduled execution and mirrors the local Docker/dbt task order. The default reviewer path remains `make run`, while the DAG documents how the same pipeline would be scheduled with logical dates, retries, catchup, and serialized SCD2 mutations in an Airflow environment.

| Setting | Value | Purpose |
|---|---|---|
| `schedule` | `0 6 * * *` | Daily batch schedule. |
| `catchup` | `True` | Allows missed logical dates to be backfilled. |
| `max_active_runs` | `1` | Serializes SCD2 mutations across dates. |
| `retries` | `3` | Retries transient extract/load/dbt failures. |
| `retry_exponential_backoff` | `True` | Backs off repeated failures instead of retrying immediately. |
| `{{ ds }}` | Passed to extract/load/dbt | Aligns Airflow logical date with warehouse `load_date`. |

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

Local Docker Compose execution remains the default reviewer path. The DAG is deployment-ready for an Airflow worker image with the same Python/dbt dependencies, but this README does not claim a production Airflow deployment.

## Engineering Scope

| Area | Implementation |
|---|---|
| Historical modeling | SCD Type 2 `dim_business` with `valid_from`, `valid_to`, and `is_current` |
| Retry safety | Advisory lock plus transactional partition replacement by `load_date` |
| Data quality | pytest and dbt tests for load behavior and SCD2 invariants |
| Bad data handling | DLQ table for unkeyable records |
| Schema drift | Header profiling before required-column failures mutate raw state |
| Orchestration design | Authored Airflow DAG with retries, catchup, logical dates, and serialized SCD2 mutations |

## How to Measure Live Scale

To replace the benchmark placeholders with real values:

1. Run the live extraction path with `make live`.
2. Capture raw row count from `raw.businesses_snapshot`.
3. Capture total versions and current rows from `public_marts.dim_business`.
4. Record runtime for extract, load, dbt run, and dbt test.
5. Update the Benchmarks table.

Suggested SQL checks:

```sql
SELECT COUNT(*) FROM raw.businesses_snapshot;
SELECT COUNT(*) FROM public_marts.dim_business;
SELECT COUNT(*) FROM public_marts.dim_business WHERE is_current = true;
SELECT load_date, row_count, rejected_count
FROM raw.businesses_snapshot_load_audit
ORDER BY load_date DESC;
```

## Known Limits and Next Benchmarks

- Local execution uses Docker Compose and PostgreSQL so reviewers can run the full pipeline without cloud cost.
- The committed 2,000-row sample is intentionally small for reproducibility; live API extraction is supported separately and should be used for scale benchmarks.
- The Airflow DAG is authored for scheduled execution but is not required for the default local `make run` path.
- The current SCD2 repair strategy rebuilds effective ranges from staged history, which is appropriate for this project scale. Larger dimensions would require incremental or partition-aware repair.
- Next benchmark target: run the live NYC Open Data extract and publish row count, runtime, dbt test duration, and SCD2 version counts.

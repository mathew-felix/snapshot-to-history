# AWS/PostgreSQL SCD2 Warehouse with dbt Core and Airflow Failure Recovery

## 1. System Architecture & Fault-Tolerant Topology

```text
                         AWS / External Boundary
[ NYC Open Data / Socrata API ]
  - full daily current-state snapshot
  - unstable network, schema drift, late data
                                |
                                | HTTPS extract, paginated, deterministic sort
                                v
[ Airflow Worker / Batch Runtime ]
  task: extract_snapshot
  output: s3://bucket/bronze/businesses/snapshot_date={{ ds }}/snapshot.csv
  metadata: airflow.task_instance, ingest_run table, structured logs
                                |
                                | COPY into transactional raw partition
                                v
[ Bronze: PostgreSQL raw schema ]
  raw.businesses_snapshot
  raw.businesses_snapshot_load_audit
  raw.businesses_snapshot_dlq

  Contract: all source columns stored as TEXT plus load_date, source_file_uri,
  run_id, ingested_at. The raw load is replace-by-load_date, not append-blind.
                                |
                                | dbt Core compile + run --select staging
                                v
[ Silver: PostgreSQL staging schema ]
  staging.stg_businesses
  staging.rejected_businesses
  staging.schema_drift_events

  Contract: typed columns, normalized tracked attributes, one row per
  (license_nbr, load_date), attr_hash, reject_reason for bad rows.
                                |
                                | dbt snapshot / SCD2 merge transaction
                                v
[ Silver SCD2: PostgreSQL marts schema ]
  marts.businesses_snapshot / marts.dim_business
  valid_from, valid_to, is_current, attr_hash, dbt_scd_id

  Contract: one current row per license_nbr; non-overlapping effective ranges.
                                |
                                | dbt run --select marts + dbt test
                                v
[ Gold: Analytical marts ]
  marts.vw_address_changes
  marts.vw_status_history
  marts.quality_summary
  marts.pipeline_run_metrics
```

Airflow owns orchestration state. The scheduler records each logical run in the metadata database as a `DagRun` keyed by `dag_id` and `logical_date`; each task records `try_number`, `state`, `start_date`, `end_date`, retry count, and log URI. If an Airflow worker dies mid-task, the scheduler observes heartbeat loss, marks the task as failed or up-for-retry, and re-queues it without losing the DAG's logical date.

dbt Core owns transformation compilation and data-quality enforcement. Airflow calls `dbt deps`, `dbt compile`, `dbt snapshot`, `dbt run`, and `dbt test` from a controlled working directory. dbt writes compiled SQL and execution artifacts into `dbt/target/`, specifically `manifest.json`, `run_results.json`, `sources.json`, and compiled SQL. Those artifacts must be shipped to S3 or durable storage after each run because container-local `target/` files disappear when an ephemeral worker is replaced.

PostgreSQL owns durable data state. Raw, staging, SCD2, DLQ, and audit tables live in PostgreSQL transactions. The pipeline does not trust local files as state. Every mutable batch step writes an audit row with `dag_run_id`, `logical_date`, `load_date`, `source_file_uri`, `row_count`, `checksum`, `status`, `started_at`, and `finished_at`. Recovery after a crash is based on database-visible state, not guesses from the filesystem.

Recommended AWS topology:

- Airflow: MWAA or self-managed Airflow on ECS/Fargate with remote logs enabled to CloudWatch/S3.
- Object storage: S3 bronze landing bucket partitioned by `snapshot_date=YYYY-MM-DD`.
- Warehouse: Amazon RDS PostgreSQL or Aurora PostgreSQL in private subnets.
- Network: Airflow workers run in the same VPC as PostgreSQL. PostgreSQL has no public ingress. Socrata/API egress flows through NAT. S3 access uses a gateway endpoint where possible.
- Secrets: Airflow connection IDs point to AWS Secrets Manager or environment-backed connections, not plaintext repository values.
- State preservation: Airflow metadata DB persists orchestration state; PostgreSQL persists warehouse state; S3 persists raw files and dbt artifacts.

## 2. Airflow Orchestration & Production Run Schedule

Daily production DAG configuration:

```python
from datetime import datetime, timedelta
from airflow import DAG

DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(hours=2),
    "execution_timeout": timedelta(hours=1),
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id="business_registry_scd2_daily",
    description="Daily NYC business snapshot ingestion and SCD2 warehouse build",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    tags=["postgres", "dbt", "scd2", "batch"],
) as dag:
    ...
```

Use `catchup=True` because SCD2 history is date-sensitive and missed logical dates must be replayable. Use `max_active_runs=1` for this dimension because concurrent historical mutations can create overlapping validity ranges if two dates write the same natural key out of order. Backfills should be submitted with explicit date windows:

```bash
airflow dags backfill business_registry_scd2_daily \
  --start-date 2026-06-01 \
  --end-date 2026-06-07
```

Pass the Airflow logical date into every task as the source of truth:

```bash
python -m src.extract --snapshot-date "{{ ds }}" --output-uri "{{ ti.xcom_pull(...) }}"
python -m src.load_raw --snapshot-date "{{ ds }}" --source-uri "s3://..."
dbt snapshot --vars '{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}'
dbt run --vars '{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}'
dbt test --vars '{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}'
```

Production task graph:

```text
validate_runtime_config
        |
extract_snapshot_to_s3
        |
profile_source_schema
        |
load_raw_replace_partition
        |
dbt_deps_compile
        |
dbt_source_freshness
        |
dbt_run_staging
        |
dbt_snapshot_scd2
        |
dbt_run_gold_marts
        |
dbt_test_invariants
        |
publish_run_summary
        |
archive_dbt_artifacts
```

Idempotency strategy:

- The natural batch key is `load_date = {{ ds }}`. Every table touched by a batch must be addressable by `load_date`, `snapshot_date`, or `airflow_run_id`.
- Bronze load uses partition replacement semantics:

```sql
BEGIN;

SELECT pg_advisory_xact_lock(hashtext('business_registry_scd2:' || :load_date));

DELETE FROM raw.businesses_snapshot
WHERE load_date = :load_date;

COPY raw.businesses_snapshot (...) FROM STDIN WITH CSV HEADER;

INSERT INTO raw.businesses_snapshot_load_audit (...)
VALUES (...)
ON CONFLICT (load_date)
DO UPDATE SET
  source_file_uri = EXCLUDED.source_file_uri,
  row_count = EXCLUDED.row_count,
  checksum = EXCLUDED.checksum,
  status = 'loaded',
  updated_at = now();

COMMIT;
```

- Staging models must be deterministic for the same `load_date`. Deduplicate with `row_number() over (partition by license_nbr, load_date order by ingested_at desc, source_row_number desc) = 1`.
- SCD2 writes compare normalized `attr_hash` against the current version. Re-running the same `load_date` must not open a new version when `attr_hash` is unchanged.
- Backfills must process dates in ascending order. Airflow enforces this with `max_active_runs=1`; SQL additionally protects critical sections with `pg_advisory_xact_lock`.
- Downstream audit and metrics tables use `ON CONFLICT (dag_id, logical_date, task_id)` or `ON CONFLICT (load_date, metric_name)` upserts. Never append operational logs into warehouse tables without a deterministic uniqueness key.
- dbt tests are the final idempotency gate: `assert_one_current_per_key`, `assert_no_overlapping_ranges`, and a rerun assertion that row counts and `checksum_agg(attr_hash)` are unchanged for the same `load_date`.

## 3. The "Chaos Engineering" Strategy: Controlled Failures & Recoveries

The commit history should intentionally show a production incident arc: introduce the bug in a small scoped commit, capture failing logs/tests, then fix it with a realistic patch. Do not hide the broken state. Tag the commits with incident-style messages so a reviewer can inspect the operational story.

### Failure Scenario A: Upstream Schema Drift

**3-stage Git timeline**

| Stage | Commit | Intent | Expected Evidence |
|---|---|---|---|
| 1 | `feat: add daily Airflow DAG for business snapshot load` | Baseline working DAG and dbt run. | Airflow green run, dbt tests pass. |
| 2 | `test-chaos: simulate upstream schema drift in raw extract` | Add a fixture where the source deletes `license_status` or adds `inspection_grade`. | `dbt run --select staging` fails or schema profile flags unexpected columns. |
| 3 | `fix: quarantine schema drift rows and persist drift events` | Add permissive raw ingestion, schema profiling, and DLQ routing. | Airflow run succeeds with warnings; DLQ/drift tables show row counts and reason codes. |

**The Crash**

The source silently changes its payload. If the extractor writes a CSV missing a required column, `COPY` can fail with `missing data for column` or dbt staging can fail during compilation/execution with `column "license_status" does not exist`. Airflow marks `load_raw_replace_partition` or `dbt_run_staging` as `failed`, retries according to policy, then stops the DAG before SCD2 mutation.

**The Mitigation**

Raw ingestion must be permissive and schema-aware:

- Store raw payload as text columns plus an optional `raw_payload JSONB` for unexpected fields.
- Add `profile_source_schema` before `COPY`. It compares incoming headers to an expected contract table.
- Required natural-key fields failing contract go to `raw.businesses_snapshot_dlq`.
- Non-critical new columns are stored in `raw_payload` and logged in `staging.schema_drift_events`.
- dbt source tests validate required fields after ingestion:

```yaml
sources:
  - name: raw
    tables:
      - name: businesses_snapshot
        columns:
          - name: license_nbr
            tests:
              - not_null
          - name: load_date
            tests:
              - not_null
```

**The Git Fix Commit**

Patch should modify:

- `src/extract.py`: writes header profile and preserves unknown fields.
- `src/load_raw.py`: accepts missing non-key columns by defaulting to `NULL`; routes unkeyable rows to DLQ.
- `sql/init/*`: creates `raw.businesses_snapshot_dlq` and `staging.schema_drift_events`.
- `dbt/models/staging/stg_businesses.sql`: uses explicit `coalesce(column, null)` behavior and does not assume optional columns exist without a compatibility shim.
- `tests/`: adds fixture for missing/added column and asserts the DAG/dbt run fails before the fix and succeeds after the fix.

### Failure Scenario B: Late-Arriving Records & Date Out-of-Bounds

**3-stage Git timeline**

| Stage | Commit | Intent | Expected Evidence |
|---|---|---|---|
| 1 | `feat: implement hash-based SCD2 snapshot` | Baseline SCD2 using daily load order. | One current row per key; no overlaps on in-order data. |
| 2 | `test-chaos: inject backdated address correction` | Load `2026-06-10` after `2026-06-15` with an older effective date. | dbt test catches overlapping ranges or duplicate current row. |
| 3 | `fix: rebuild affected SCD2 keys for late-arriving records` | Add impacted-key repair strategy. | Backfill run repairs history without truncating the whole dimension. |

**The Crash**

Default SCD2 logic assumes records arrive in chronological order. A backdated correction can try to insert a version with `valid_from = 2026-06-10` while the current row already starts at `2026-06-15`. Naive logic either ignores the older row, opens a second current row, or creates invalid windows such as `valid_from > valid_to`. dbt singular tests should fail:

```sql
select license_nbr
from marts.dim_business d1
join marts.dim_business d2
  on d1.license_nbr = d2.license_nbr
 and d1.business_sk <> d2.business_sk
 and d1.valid_from < coalesce(d2.valid_to, date '9999-12-31')
 and d2.valid_from < coalesce(d1.valid_to, date '9999-12-31')
```

**The Mitigation**

Use an impacted-key rebuild strategy instead of blind append-only SCD2 mutation:

- Detect late arrivals where `load_date < max(valid_from)` for the same `license_nbr`.
- Write impacted keys to `staging.scd2_repair_keys`.
- Inside a transaction, delete only the affected keys from the SCD2 table.
- Recompute the full history for those keys from all available staged snapshots ordered by `load_date`.
- Derive windows with `lead(load_date) over (partition by license_nbr order by load_date)` and set `valid_to` to the next change date.
- Set `is_current = valid_to is null`.

The dbt implementation can be a custom incremental model or macro invoked after staging:

```sql
{{ rebuild_scd2_for_impacted_keys(
    source_relation=ref('stg_businesses'),
    target_relation=ref('dim_business'),
    key_column='license_nbr',
    effective_date_column='load_date',
    hash_column='attr_hash'
) }}
```

For dbt snapshots specifically, document the limitation and add a controlled repair model that materializes `marts.dim_business` from snapshot history rather than relying on snapshot append order alone.

**The Git Fix Commit**

Patch should modify:

- `dbt/macros/rebuild_scd2_for_impacted_keys.sql`: encapsulates affected-key repair.
- `dbt/models/staging/scd2_repair_keys.sql`: identifies late-arriving keys.
- `dbt/models/marts/dim_business.sql`: rebuilds valid ranges from ordered staged states.
- `dbt/tests/assert_no_overlapping_ranges.sql`: remains the invariant gate.
- `tests/`: adds a late-arrival fixture and asserts repaired history has exact windows.

### Failure Scenario C: Memory Exhaustion / Network Timeout on Compute

**3-stage Git timeline**

| Stage | Commit | Intent | Expected Evidence |
|---|---|---|---|
| 1 | `feat: load full snapshot through Airflow worker` | Baseline task uses single large extract/load step. | Works on normal sample data. |
| 2 | `test-chaos: force network timeout during raw copy` | Simulate database disconnect or worker timeout mid-batch. | Airflow task retries; raw table is not partially committed. |
| 3 | `fix: add chunked loads, transactional staging, and exponential backoff` | Make retry safe and self-healing. | Retry succeeds; audit table shows failed attempt and successful replacement. |

**The Crash**

The worker loses its PostgreSQL connection halfway through `COPY`, the database restarts, or the task exceeds `execution_timeout`. Without explicit transaction boundaries, the pipeline can leave a half-loaded raw snapshot or an audit row marked successful even though dbt sees incomplete data. Airflow records the task as failed; retrying an append-only load then doubles whatever was already inserted.

**The Mitigation**

- Wrap delete-and-copy in one database transaction. PostgreSQL rolls back partial `COPY` on connection loss when the transaction does not commit.
- Load into a temporary or run-scoped staging table first:

```sql
CREATE TABLE raw.businesses_snapshot_stage_{{ run_id_hash }}
(LIKE raw.businesses_snapshot INCLUDING DEFAULTS);
```

- Validate staged row count and checksum before replacing the production partition.
- Swap with delete/insert in one transaction.
- Use Airflow retry settings:

```python
retries=5
retry_delay=timedelta(minutes=5)
retry_exponential_backoff=True
max_retry_delay=timedelta(minutes=45)
execution_timeout=timedelta(minutes=45)
```

- Use task-level resource limits for dbt:

```bash
dbt run --threads 2 --fail-fast --select staging marts
```

- Persist failure telemetry:

```sql
insert into marts.pipeline_run_metrics
(dag_id, logical_date, task_id, try_number, status, error_class, rows_loaded, duration_seconds)
values (...)
on conflict (dag_id, logical_date, task_id, try_number) do update ...
```

**The Git Fix Commit**

Patch should modify:

- `dags/business_registry_scd2_daily.py`: increases retries, enables exponential backoff, sets `execution_timeout`, and archives logs/artifacts.
- `src/load_raw.py`: uses run-scoped staging table, explicit transaction, checksum validation, and advisory lock.
- `sql/init/*`: adds load audit indexes and uniqueness constraints.
- `tests/test_load_raw.py`: simulates failure before commit and proves no partial rows persist.

## 4. Observability & Recruiter Talking Points

Log surfaces:

- Airflow UI: task state, Gantt view, retry attempts, duration, exception stack traces, rendered templates, and per-task logs.
- CloudWatch/S3 remote logs: durable logs for ephemeral workers.
- dbt artifacts: `target/run_results.json` for model status/timing, `target/manifest.json` for lineage, `target/sources.json` for freshness, and `target/compiled/` for exact SQL shipped to PostgreSQL.
- PostgreSQL audit tables: `raw.businesses_snapshot_load_audit`, `staging.schema_drift_events`, `raw.businesses_snapshot_dlq`, and `marts.pipeline_run_metrics`.
- dbt tests: explicit pass/fail records for SCD2 invariants and source contract checks.

Operational metrics to track:

- Mean Time to Detect: minutes between task start and first failed Airflow/dbt signal.
- Mean Time to Recover: minutes between first failure and successful rerun for the same `logical_date`.
- Pipeline SLA breach count: number of DAG runs finishing after the business freshness deadline.
- Retry rate by task: high retry rate on extract/load indicates upstream or network instability.
- DLQ row count and DLQ percentage by `load_date`.
- Schema drift event count by source column.
- SCD2 churn rate: changed keys divided by total keys; spikes indicate true source event or normalization bug.
- Current-row invariant failures: count of keys with more than one `is_current = true`.
- Range-overlap failures: count of keys with overlapping effective windows.
- dbt model runtime p95 and p99.
- Rows loaded versus source-reported row count.

Concrete interview talking points:

1. "I designed the DAG around Airflow logical dates, not wall-clock execution time. That made daily runs and backfills deterministic: the same `{{ ds }}` replaces the same raw partition, feeds the same dbt variables, and either produces the same SCD2 state or fails an invariant test."

2. "I intentionally committed broken pipeline states for schema drift, late-arriving records, and mid-load failure. The value was not the failure itself; it was proving the recovery boundary. Raw data is replaceable by partition, SCD2 history is protected by dbt tests, and partial loads cannot commit because PostgreSQL transactions and advisory locks define the critical section."

3. "The most production-like part of the project is the audit trail. Airflow tells me which task failed, dbt tells me which model or test failed, and warehouse audit tables tell me what data changed. That gives me a real incident narrative: detection, blast radius, mitigation, fix commit, and verification."

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator


PROJECT_DIR = Path(__file__).resolve().parents[1]
DBT_DIR = PROJECT_DIR / "dbt"

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


def project_bash(task_id: str, command: str) -> BashOperator:
    return BashOperator(
        task_id=task_id,
        bash_command=f"cd {PROJECT_DIR.as_posix()} && {command}",
        env={
            "AIRFLOW_DAG_ID": "{{ dag.dag_id }}",
            "AIRFLOW_RUN_ID": "{{ run_id }}",
            "AIRFLOW_TASK_ID": "{{ task.task_id }}",
            "SNAPSHOT_DATE": "{{ ds }}",
        },
        append_env=True,
    )


with DAG(
    dag_id="business_registry_scd2_daily",
    description="Daily NYC business snapshot ingestion and SCD2 warehouse build",
    start_date=datetime(2026, 1, 1),
    schedule="0 6 * * *",
    catchup=True,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=3),
    tags=["postgres", "dbt", "scd2", "batch", "portfolio"],
) as dag:
    validate_runtime_config = EmptyOperator(task_id="validate_runtime_config")

    extract_snapshot_to_local = project_bash(
        "extract_snapshot_to_local",
        'python -m src.extract --snapshot-date "{{ ds }}"',
    )

    profile_source_schema = project_bash(
        "profile_source_schema",
        'python -m src.load_raw --csv "data/raw/snapshot_{{ ds }}.csv" '
        '--date "{{ ds }}" --profile-only',
    )

    load_raw_replace_partition = project_bash(
        "load_raw_replace_partition",
        'python -m src.load_raw --csv "data/raw/snapshot_{{ ds }}.csv" '
        '--date "{{ ds }}"',
    )

    dbt_deps_compile = project_bash(
        "dbt_deps_compile",
        f"cd {DBT_DIR.as_posix()} && dbt deps --profiles-dir . && "
        'dbt compile --profiles-dir . --vars \'{"snapshot_date": "{{ ds }}", '
        '"airflow_run_id": "{{ run_id }}"}\'',
    )

    dbt_source_freshness = project_bash(
        "dbt_source_freshness",
        f"cd {DBT_DIR.as_posix()} && dbt source freshness --profiles-dir . "
        "--select source:raw.businesses_snapshot || true",
    )

    dbt_run_staging = project_bash(
        "dbt_run_staging",
        f"cd {DBT_DIR.as_posix()} && dbt run --profiles-dir . "
        "--select staging --vars "
        '\'{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}\'',
    )

    dbt_snapshot_scd2 = project_bash(
        "dbt_snapshot_scd2",
        f"cd {DBT_DIR.as_posix()} && dbt snapshot --profiles-dir . --vars "
        '\'{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}\'',
    )

    dbt_run_gold_marts = project_bash(
        "dbt_run_gold_marts",
        f"cd {DBT_DIR.as_posix()} && dbt run --profiles-dir . "
        "--select marts --vars "
        '\'{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}\'',
    )

    dbt_test_invariants = project_bash(
        "dbt_test_invariants",
        f"cd {DBT_DIR.as_posix()} && dbt test --profiles-dir . --vars "
        '\'{"snapshot_date": "{{ ds }}", "airflow_run_id": "{{ run_id }}"}\'',
    )

    publish_run_summary = project_bash(
        "publish_run_summary",
        "python -m src.summary",
    )

    archive_dbt_artifacts = project_bash(
        "archive_dbt_artifacts",
        'python -m src.archive_dbt_artifacts --run-id "{{ run_id }}" --logical-date "{{ ds }}"',
    )

    (
        validate_runtime_config
        >> extract_snapshot_to_local
        >> profile_source_schema
        >> load_raw_replace_partition
        >> dbt_deps_compile
        >> dbt_source_freshness
        >> dbt_run_staging
        >> dbt_snapshot_scd2
        >> dbt_run_gold_marts
        >> dbt_test_invariants
        >> publish_run_summary
        >> archive_dbt_artifacts
    )


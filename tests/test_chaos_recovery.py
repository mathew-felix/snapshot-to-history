import csv
import io
import subprocess
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from src import load_raw


CHAOS_DATE = date(2099, 2, 10)
CHAOS_LATE_DATE = date(2099, 2, 15)
CHAOS_KEY = "CHAOS-001"


def write_csv(path: Path, rows: list[dict]):
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(output.getvalue(), encoding="utf-8")


def business_row(address_street: str, load_key: str = CHAOS_KEY) -> dict:
    return {
        "license_nbr": load_key,
        "business_name": "Chaos Coffee LLC",
        "business_name2": "",
        "address_building": "10",
        "address_street": address_street,
        "address_city": "New York",
        "address_state": "NY",
        "address_zip": "10001",
        "license_status": "Active",
        "license_category": "Food",
        "license_creation_date": "2020-01-01T00:00:00",
    }


@pytest.fixture(autouse=True)
def cleanup_chaos_rows(db_conn):
    dates = (CHAOS_DATE, CHAOS_LATE_DATE)
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM raw.businesses_snapshot WHERE load_date IN %s", (dates,))
        cur.execute("DELETE FROM raw.businesses_snapshot_dlq WHERE load_date IN %s", (dates,))
        cur.execute("DELETE FROM raw.businesses_snapshot_load_audit WHERE load_date IN %s", (dates,))
        cur.execute("DELETE FROM staging.schema_drift_events WHERE load_date IN %s", (dates,))
    db_conn.commit()
    yield
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM raw.businesses_snapshot WHERE load_date IN %s", (dates,))
        cur.execute("DELETE FROM raw.businesses_snapshot_dlq WHERE load_date IN %s", (dates,))
        cur.execute("DELETE FROM raw.businesses_snapshot_load_audit WHERE load_date IN %s", (dates,))
        cur.execute("DELETE FROM staging.schema_drift_events WHERE load_date IN %s", (dates,))
    db_conn.commit()


def run_dbt_models():
    subprocess.run(
        [
            "dbt",
            "run",
            "--profiles-dir",
            ".",
            "--select",
            "staging",
            "dim_business",
        ],
        cwd="dbt",
        check=True,
    )
    subprocess.run(
        [
            "dbt",
            "test",
            "--profiles-dir",
            ".",
            "--select",
            "assert_one_current_per_key",
            "assert_no_overlapping_ranges",
        ],
        cwd="dbt",
        check=True,
    )


def test_required_schema_drift_fails_before_raw_mutation(db_conn, tmp_path):
    drift_file = tmp_path / "missing_required_column.csv"
    write_csv(
        drift_file,
        [
            {
                "business_name": "No Key Column LLC",
                "address_street": "Broken Contract",
            }
        ],
    )

    with pytest.raises(ValueError, match="Required source columns are missing"):
        load_raw.load_snapshot(str(drift_file), CHAOS_DATE)

    with db_conn.cursor() as cur:
        cur.execute(
            "SELECT count(*) FROM raw.businesses_snapshot WHERE load_date = %s",
            (CHAOS_DATE,),
        )
        raw_count = cur.fetchone()[0]
        cur.execute(
            """
            SELECT status
            FROM staging.schema_drift_events
            WHERE load_date = %s
            ORDER BY detected_at DESC
            LIMIT 1
            """,
            (CHAOS_DATE,),
        )
        status = cur.fetchone()[0]

    assert raw_count == 0
    assert status == "failed"


def test_late_arriving_record_rebuilds_valid_ranges(db_conn, tmp_path):
    newer_file = tmp_path / "newer.csv"
    older_file = tmp_path / "older.csv"
    write_csv(newer_file, [business_row("Broadway")])
    write_csv(older_file, [business_row("Main St")])

    load_raw.load_snapshot(str(newer_file), CHAOS_LATE_DATE)
    load_raw.load_snapshot(str(older_file), CHAOS_DATE)
    run_dbt_models()

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT address_full, valid_from, valid_to, is_current
            FROM public_marts.dim_business
            WHERE license_nbr = %s
            ORDER BY valid_from
            """,
            (CHAOS_KEY,),
        )
        rows = cur.fetchall()

    assert len(rows) == 2
    assert rows[0][1] == CHAOS_DATE
    assert rows[0][2] == CHAOS_LATE_DATE
    assert rows[0][3] is False
    assert rows[1][1] == CHAOS_LATE_DATE
    assert rows[1][2] is None
    assert rows[1][3] is True


def test_failed_load_rolls_back_existing_partition(db_conn, tmp_path):
    original_file = tmp_path / "original.csv"
    replacement_file = tmp_path / "replacement.csv"
    write_csv(original_file, [business_row("Main St", "ROLLBACK-001")])
    write_csv(replacement_file, [business_row("Broadway", "ROLLBACK-002")])

    load_raw.load_snapshot(str(original_file), CHAOS_DATE)

    with patch("src.load_raw.file_checksum", side_effect=RuntimeError("forced checksum failure")):
        with pytest.raises(RuntimeError, match="forced checksum failure"):
            load_raw.load_snapshot(str(replacement_file), CHAOS_DATE)

    with db_conn.cursor() as cur:
        cur.execute(
            """
            SELECT license_nbr
            FROM raw.businesses_snapshot
            WHERE load_date = %s
            ORDER BY license_nbr
            """,
            (CHAOS_DATE,),
        )
        loaded_keys = [row[0] for row in cur.fetchall()]

    assert loaded_keys == ["ROLLBACK-001"]


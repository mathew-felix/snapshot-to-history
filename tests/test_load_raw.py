"""
test_load_raw.py — integration tests for the idempotent raw ingestion.
Requires a live Postgres connection (the Docker db service).
"""

import csv
import io
from datetime import date
from unittest.mock import patch, mock_open

import pytest
import psycopg2

from src.load_raw import load_snapshot


TEST_DATE = date(2099, 1, 1)   # far-future date so tests don't clash with real data

SAMPLE_ROWS = [
    {
        "license_nbr": "TEST-001",
        "business_name": "Test Biz A",
        "business_name2": "",
        "address_building": "10",
        "address_street": "Main St",
        "address_city": "New York",
        "address_state": "NY",
        "address_zip": "10001",
        "license_status": "Active",
        "license_category": "Food",
        "license_creation_date": "2020-01-01T00:00:00",
    },
    {
        "license_nbr": "TEST-002",
        "business_name": "Test Biz B",
        "business_name2": "",
        "address_building": "20",
        "address_street": "Broadway",
        "address_city": "New York",
        "address_state": "NY",
        "address_zip": "10002",
        "license_status": "Active",
        "license_category": "Retail",
        "license_creation_date": "2021-06-15T00:00:00",
    },
]


def make_csv_content(rows):
    """Build a CSV string from a list of dicts."""
    output = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(output, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return output.getvalue()


@pytest.fixture(autouse=True)
def cleanup_test_date(db_conn):
    """Remove test rows before and after each test."""
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM raw.businesses_snapshot WHERE load_date = %s", (TEST_DATE,))
    db_conn.commit()
    yield
    with db_conn.cursor() as cur:
        cur.execute("DELETE FROM raw.businesses_snapshot WHERE load_date = %s", (TEST_DATE,))
    db_conn.commit()


class TestLoadRawIdempotency:
    def test_first_load_inserts_rows(self, db_conn, tmp_path):
        """Loading a CSV inserts the expected number of rows."""
        csv_file = tmp_path / "test_snap.csv"
        csv_file.write_text(make_csv_content(SAMPLE_ROWS), encoding="utf-8")

        load_snapshot(str(csv_file), TEST_DATE)

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM raw.businesses_snapshot WHERE load_date = %s",
                (TEST_DATE,)
            )
            count = cur.fetchone()[0]

        assert count == len(SAMPLE_ROWS), f"Expected {len(SAMPLE_ROWS)}, got {count}"

    def test_reload_same_date_is_idempotent(self, db_conn, tmp_path):
        """Running the loader twice for the same date keeps the row count the same."""
        csv_file = tmp_path / "test_snap.csv"
        csv_file.write_text(make_csv_content(SAMPLE_ROWS), encoding="utf-8")

        load_snapshot(str(csv_file), TEST_DATE)
        load_snapshot(str(csv_file), TEST_DATE)  # second run

        with db_conn.cursor() as cur:
            cur.execute(
                "SELECT count(*) FROM raw.businesses_snapshot WHERE load_date = %s",
                (TEST_DATE,)
            )
            count = cur.fetchone()[0]

        assert count == len(SAMPLE_ROWS), (
            f"Idempotency broken: expected {len(SAMPLE_ROWS)}, got {count}"
        )

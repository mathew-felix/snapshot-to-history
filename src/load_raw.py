import argparse
import csv
import hashlib
from datetime import date, datetime, timezone

import psycopg2

from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

EXPECTED_COLUMNS = [
    "license_nbr",
    "business_name",
    "business_name2",
    "address_building",
    "address_street",
    "address_city",
    "address_state",
    "address_zip",
    "license_status",
    "license_category",
    "license_creation_date",
]

REQUIRED_COLUMNS = {"license_nbr"}


def profile_schema(csv_path: str, load_date: date):
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []

    missing_columns = sorted(set(EXPECTED_COLUMNS) - set(headers))
    added_columns = sorted(set(headers) - set(EXPECTED_COLUMNS))
    status = "failed" if REQUIRED_COLUMNS.intersection(missing_columns) else "warning"
    if not missing_columns and not added_columns:
        status = "passed"

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO staging.schema_drift_events (
                        load_date, source_file_uri, expected_columns,
                        observed_columns, missing_columns, added_columns, status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        load_date,
                        csv_path,
                        EXPECTED_COLUMNS,
                        headers,
                        missing_columns,
                        added_columns,
                        status,
                    ),
                )
    finally:
        conn.close()

    if status == "failed":
        raise ValueError(f"Required source columns are missing: {missing_columns}")

    print(
        "Schema profile "
        f"{status}: missing={missing_columns or []}, added={added_columns or []}"
    )
    return {
        "status": status,
        "missing_columns": missing_columns,
        "added_columns": added_columns,
    }


def file_checksum(csv_path: str) -> str:
    digest = hashlib.sha256()
    with open(csv_path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def load_snapshot(csv_path: str, load_date: date = None):
    load_date = load_date or date.today()
    ingested_at = datetime.now(timezone.utc)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT pg_advisory_xact_lock(hashtext(%s))",
                    (f"business_registry_scd2:{load_date}",),
                )

                profile_schema(csv_path, load_date)

                # IDEMPOTENCY: delete this date's data before loading
                # so re-running the same snapshot never creates duplicates
                cur.execute(
                    "DELETE FROM raw.businesses_snapshot WHERE load_date = %s",
                    (load_date,)
                )
                deleted = cur.rowcount
                if deleted:
                    print(f"Removed {deleted} existing rows for {load_date} (re-run detected)")

                # Read CSV and insert rows
                with open(csv_path, "r", encoding="utf-8", newline="") as f:
                    reader = csv.DictReader(f)
                    rows = []
                    rejects = []
                    for source_row_number, row in enumerate(reader, start=2):
                        if not (row.get("license_nbr") or "").strip():
                            rejects.append(
                                (
                                    load_date,
                                    csv_path,
                                    source_row_number,
                                    "missing_license_nbr",
                                    str(row),
                                    ingested_at,
                                )
                            )
                            continue

                        rows.append((
                            row.get("license_nbr"),
                            row.get("business_name"),
                            row.get("business_name2"),
                            row.get("address_building"),
                            row.get("address_street"),
                            row.get("address_city"),
                            row.get("address_state"),
                            row.get("address_zip"),
                            row.get("license_status"),
                            row.get("license_category"),
                            row.get("license_creation_date"),
                            load_date,
                            ingested_at,
                        ))

                cur.executemany(
                    """
                    INSERT INTO raw.businesses_snapshot (
                        license_nbr, business_name, business_name2,
                        address_building, address_street, address_city,
                        address_state, address_zip, license_status,
                        license_category, license_creation_date,
                        load_date, ingested_at
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    rows,
                )
                if rejects:
                    cur.executemany(
                        """
                        INSERT INTO raw.businesses_snapshot_dlq (
                            load_date, source_file_uri, source_row_number,
                            reject_reason, raw_payload, rejected_at
                        ) VALUES (%s,%s,%s,%s,%s,%s)
                        """,
                        rejects,
                    )

                cur.execute(
                    """
                    INSERT INTO raw.businesses_snapshot_load_audit (
                        load_date, source_file_uri, row_count, rejected_count,
                        checksum, status, loaded_at
                    )
                    VALUES (%s, %s, %s, %s, %s, 'loaded', %s)
                    ON CONFLICT (load_date)
                    DO UPDATE SET
                        source_file_uri = EXCLUDED.source_file_uri,
                        row_count = EXCLUDED.row_count,
                        rejected_count = EXCLUDED.rejected_count,
                        checksum = EXCLUDED.checksum,
                        status = EXCLUDED.status,
                        loaded_at = EXCLUDED.loaded_at
                    """,
                    (
                        load_date,
                        csv_path,
                        len(rows),
                        len(rejects),
                        file_checksum(csv_path),
                        ingested_at,
                    ),
                )
                print(f"Inserted {len(rows)} rows for {load_date}")
                if rejects:
                    print(f"Routed {len(rejects)} bad rows to raw.businesses_snapshot_dlq")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to snapshot CSV")
    parser.add_argument("--date", help="Load date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--profile-only", action="store_true", help="Only profile source headers")
    args = parser.parse_args()
    load_date = date.fromisoformat(args.date) if args.date else date.today()
    if args.profile_only:
        profile_schema(args.csv, load_date)
    else:
        load_snapshot(args.csv, load_date)

import argparse
import csv
from datetime import date, datetime, timezone

import psycopg2

from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def load_snapshot(csv_path: str, load_date: date = None):
    load_date = load_date or date.today()
    ingested_at = datetime.now(timezone.utc)

    conn = get_connection()
    try:
        with conn:
            with conn.cursor() as cur:
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
                with open(csv_path, "r", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = [
                        (
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
                        )
                        for row in reader
                    ]

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
                print(f"Inserted {len(rows)} rows for {load_date}")
    finally:
        conn.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", required=True, help="Path to snapshot CSV")
    parser.add_argument("--date", help="Load date (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()
    load_date = date.fromisoformat(args.date) if args.date else date.today()
    load_snapshot(args.csv, load_date)

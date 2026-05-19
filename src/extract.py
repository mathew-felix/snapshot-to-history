import csv
import argparse
import os
import time
import requests
from datetime import date
from src.config import (
    SOCRATA_BASE_URL, SOCRATA_DATASET_ID,
    SOCRATA_PAGE_SIZE, SOCRATA_APP_TOKEN, RAW_DATA_DIR
)

COLUMNS = [
    "license_nbr", "business_name", "business_name2",
    "address_building", "address_street", "address_city",
    "address_state", "address_zip", "license_status",
    "license_category", "license_creation_date",
]

def fetch_snapshot(snapshot_date: date = None) -> str:
    """Pull full snapshot from Socrata API. Returns path to saved CSV."""
    snapshot_date = snapshot_date or date.today()
    output_path = os.path.join(RAW_DATA_DIR, f"snapshot_{snapshot_date}.csv")

    headers = {}
    if SOCRATA_APP_TOKEN:
        headers["X-App-Token"] = SOCRATA_APP_TOKEN

    os.makedirs(RAW_DATA_DIR, exist_ok=True)

    url = f"{SOCRATA_BASE_URL}/{SOCRATA_DATASET_ID}.json"
    all_rows = []
    offset = 0

    print(f"Downloading snapshot for {snapshot_date}...")

    while True:
        params = {
            "$limit": SOCRATA_PAGE_SIZE,
            "$offset": offset,
            "$order": "license_nbr ASC",   # stable sort = deterministic pagination
        }
        resp = requests.get(url, headers=headers, params=params, timeout=30)
        resp.raise_for_status()
        page = resp.json()

        if not page:
            break

        all_rows.extend(page)
        offset += len(page)
        print(f"  Fetched {offset} rows so far...")

        if len(page) < SOCRATA_PAGE_SIZE:
            break

        time.sleep(0.5)  # be polite to the API

    print(f"Total rows downloaded: {len(all_rows)}")

    # Write CSV
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(all_rows)

    print(f"Saved to: {output_path}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--snapshot-date",
        help="Logical snapshot date in YYYY-MM-DD format. Defaults to today.",
    )
    args = parser.parse_args()
    snapshot_date = date.fromisoformat(args.snapshot_date) if args.snapshot_date else date.today()
    fetch_snapshot(snapshot_date)

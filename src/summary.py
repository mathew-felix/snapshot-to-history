"""
summary.py — queries the pipeline results and prints a console report.
Called as the final step in run.sh.
"""

import psycopg2
from src.config import DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD


def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )


def print_summary():
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            # Pull from the quality_summary mart model
            cur.execute("""
                SELECT
                    snapshot_date,
                    raw_rows_ingested,
                    total_versions,
                    current_rows,
                    unique_current_businesses
                FROM public_marts.quality_summary
                LIMIT 1
            """)
            row = cur.fetchone()

            if not row:
                print("No summary data found. Has the pipeline run yet?")
                return

            snapshot_date, raw_rows, total_versions, current_rows, unique_biz = row

            # Count dbt tests passed by querying pg_stat_user_tables as a proxy
            # (dbt doesn't write test results to DB; we just report what we know)
            cur.execute("""
                SELECT count(*)
                FROM public_marts.dim_business
                WHERE is_current
            """)
            current_check = cur.fetchone()[0]

        print()
        print("=========== SCD2 BUSINESS REGISTRY — RUN SUMMARY ===========")
        print(f"Snapshot date             : {snapshot_date}")
        print(f"Raw rows ingested         : {raw_rows:,}")
        print(f"Total versions in history : {total_versions:,}")
        print(f"Current rows (is_current) : {current_rows:,}")
        print(f"Unique active businesses  : {unique_biz:,}")
        print(f"Current row check (direct): {current_check:,}")
        print("=============================================================")
        print()

    finally:
        conn.close()


if __name__ == "__main__":
    print_summary()

#!/usr/bin/env bash
set -euo pipefail

echo "==> Initializing (schemas applied via docker-entrypoint-initdb.d)"

if [ "${USE_LIVE:-0}" = "1" ]; then
  echo "==> Extracting live snapshot from Socrata"
  python -m src.extract
  SNAPSHOT_CSV="$(ls -t data/raw/snapshot_*.csv | head -1)"
else
  echo "==> Using committed sample snapshot"
  SNAPSHOT_CSV="data/sample/snapshot_sample.csv"
fi

echo "==> Loading raw snapshot (idempotent)"
python -m src.load_raw --csv "$SNAPSHOT_CSV"

echo "==> dbt deps / snapshot / run / test"
cd dbt
dbt deps --profiles-dir .
dbt snapshot --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
cd ..

echo "==> Run summary"
python -m src.summary

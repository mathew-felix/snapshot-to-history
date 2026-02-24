# SCD Type 2 Business Registry Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-17%20passing-brightgreen)

**Turns a stateless NYC business license feed into a queryable point-in-time history using SCD Type 2.**

> NYC publishes only a daily "current state" snapshot of ~70K business licenses with no history — every overwrite destroys the past. This pipeline ingests full snapshots into PostgreSQL and uses dbt to maintain a versioned history of every business record, so questions like *"What was this business's address on March 1st?"* become answerable.

---

## Quickstart

```bash
git clone https://github.com/mathew-felix/snapshot-to-history.git
cd snapshot-to-history
cp .env.example .env
make run
```

Postgres starts, schemas initialize, the sample dataset loads, dbt runs, tests pass, and a summary prints — no manual steps.

---

## The Problem

NYC's [active business license dataset](https://data.cityofnewyork.us/Business/Legally-Operating-Businesses/w7w3-xahh) is a full overwrite snapshot. Every daily load destroys the previous state:

- *"What was this business's address on March 1st?"* → impossible to answer
- *"How many businesses changed address last month?"* → no data exists
- Re-running the same load silently creates duplicate rows

**SCD Type 2** solves this by keeping a version row for every state a record has been in, with `valid_from` / `valid_to` dates and a `is_current` flag.

---

## How It Works

```
NYC Open Data API
      ↓  extract.py — paginated pull, stable sort
data/raw/snapshot_YYYY-MM-DD.csv
      ↓  load_raw.py — idempotent COPY into Postgres
raw.businesses_snapshot  (all columns stored as TEXT)
      ↓  dbt staging — normalize, cast, deduplicate, hash
staging.stg_businesses   (one clean row per license per day)
      ↓  dbt snapshot — compare attr_hash, open/close versions
marts.businesses_snapshot  (SCD2: valid_from, valid_to, is_current)
      ↓
Analytics views: address changes · status history · run quality metrics
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full design rationale.

---

## Key Engineering Decisions

**Normalize before hashing**
Tracked attributes are uppercased, trimmed, and whitespace-collapsed *before* computing the MD5 hash. `"Main St"` and `"MAIN  ST"` produce the same hash — preventing false-positive new versions from cosmetic source differences.

**Idempotent loads**
`load_raw.py` deletes the existing rows for a `load_date` before re-inserting. Re-running the same snapshot always produces the same result — no duplicates, no phantom versions.

**Tests prove correctness**
Two custom dbt tests enforce the SCD2 invariants on every run:
- `assert_one_current_per_key` — at most one open version per business
- `assert_no_overlapping_ranges` — no two versions overlap in time

---

## Tests

```bash
pytest tests/ -v          # 5 Python unit tests (mocked, no API calls)
cd dbt && dbt test        # 12 dbt schema + singular tests
```

---

## Tech Stack

| | |
|---|---|
| **Python 3.11** | Data extraction, idempotent loading, orchestration |
| **PostgreSQL 16** | Append-only raw store, SCD2 dimension, analytics views |
| **dbt Core 1.7** | Staging normalization, snapshot strategy, data quality tests |
| **Docker Compose** | One-command reproducible environment |

---

## What's Next

- Schedule daily runs with an **Airflow DAG**
- Close versions for businesses that **disappear from a snapshot**
- Build **fact tables** that join on `business_sk` for point-in-time analytics
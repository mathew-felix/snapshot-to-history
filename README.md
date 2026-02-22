# SCD Type 2 Business Registry Pipeline

![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791?logo=postgresql&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Tests](https://img.shields.io/badge/tests-17%20passing-brightgreen)

**Turns a stateless NYC business license feed into a queryable point-in-time history using SCD Type 2.**

```
70,000 rows  ·  idempotent loads  ·  hash-based change detection  ·  17 automated tests  ·  one-command setup
```

> NYC publishes only a daily "current state" snapshot with no history, so naive loads destroy the past. This pipeline ingests full snapshots into PostgreSQL and uses dbt to maintain a **Slowly Changing Dimension (Type 2)** — turning a stateless feed into a queryable point-in-time history, runnable end-to-end with a single `make run`.

---

## Quickstart

```bash
git clone https://github.com/mathew-felix/snapshot-to-history.git
cd snapshot-to-history
cp .env.example .env
make run
```

That's it. Postgres starts, schemas initialize, sample data loads, dbt runs, tests pass, and a summary prints.

---

## The Problem

NYC's [active business license dataset](https://data.cityofnewyork.us/Business/Legally-Operating-Businesses/w7w3-xahh) is a **full overwrite snapshot** — every load destroys the previous state. This means:

- "What was this business's address on March 1st?" → **impossible to answer**
- "How many businesses changed address last month?" → **no data**
- Re-running the same load creates duplicate rows → **broken analytics**

SCD Type 2 solves this by maintaining a versioned history row for every state a business has been in, with `valid_from`/`valid_to` timestamps and an `is_current` flag.

---

## Architecture

```
[NYC Open Data API]  →  extract.py  →  data/raw/snapshot_YYYY-MM-DD.csv
                                              ↓
                                       load_raw.py (idempotent COPY)
                                              ↓
                                  raw.businesses_snapshot  (all TEXT)
                                              ↓
                                   dbt: stg_businesses  (normalized + hashed)
                                              ↓
                                   dbt snapshot: marts.businesses_snapshot  (SCD2)
                                              ↓
                          Analytics: vw_address_changes, vw_status_history
                                              ↓
                                       src/summary.py  (console report)
```

See [ARCHITECTURE.md](ARCHITECTURE.md) for the full deep-dive.

---

## How SCD2 + Idempotency Are Guaranteed

**1. Normalize before hashing**
All tracked attributes (name, address, status) are uppercased, trimmed, and whitespace-collapsed *before* computing the MD5 hash. This means `"Main St"` and `"MAIN  ST"` produce the same hash — no false-positive new versions from cosmetic differences.

**2. Hash-based change detection**
dbt's `check` strategy compares `attr_hash` against the current dimension row. Only a real attribute change opens a new version.

**3. Idempotent raw loads**
`load_raw.py` does `DELETE WHERE load_date = :d` before `COPY` — re-running the same snapshot always produces the same raw row count.

**4. Proven by tests, not assumed**
- `assert_one_current_per_key.sql` — fails if any `license_nbr` has >1 open version
- `assert_no_overlapping_ranges.sql` — fails if any two versions for the same key overlap in time

---

## Data Contract

| Layer | Table | Key |
|---|---|---|
| Raw | `raw.businesses_snapshot` | `(license_nbr, load_date)` |
| Staging | `staging.stg_businesses` | `(license_nbr, load_date)` |
| Mart (SCD2) | `marts.businesses_snapshot` | `dbt_scd_id` (surrogate), `license_nbr` (natural) |

---

## Tests & Data Quality

```bash
# Python unit tests (mocked — no API calls)
pytest tests/ -v

# dbt schema + singular tests
cd dbt && dbt test --profiles-dir .
```

| Test | What it enforces |
|---|---|
| `test_single_page_stops` | Pagination halts on partial pages |
| `test_csv_has_correct_columns` | Output CSV matches the data contract |
| `test_api_error_raises` | HTTP errors propagate — never silent failures |
| `test_first_load_inserts_rows` | Correct row count after first load |
| `test_reload_same_date_is_idempotent` | Row count unchanged on re-run |
| `assert_one_current_per_key` | ≤1 current version per business |
| `assert_no_overlapping_ranges` | No overlapping validity windows |
| Schema `not_null` / `unique` tests | Column-level data contract |

---

## Tech Stack & Why

| Tool | Why |
|---|---|
| **Python** | Best ergonomics for HTTP pagination + file I/O + psycopg2 COPY |
| **PostgreSQL 16** | Free, ACID, supports COPY, window functions, and constraints |
| **dbt Core 1.7** | Battle-tested SCD2 via `dbt snapshot`, declarative tests, lineage graph |
| **Docker Compose** | One-command reproducible environment, no host pollution |

---

## Resume Bullet

> Built an idempotent SCD Type 2 pipeline (Python, PostgreSQL, dbt) over ~70K NYC business-license records, **measured by zero overlapping history ranges and zero duplicate versions on snapshot re-runs**, by implementing hash-based change detection with pre-hash normalization and dbt-enforced data-quality tests.

---

## What I'd Do Next

- **Airflow DAG** for daily scheduling with backfill support
- **Detect deletions** — businesses that disappear from a snapshot get `valid_to` closed
- **Fact tables** — join on `business_sk` + as-of date for point-in-time analytics
- **More sources** — join NYC permits and inspections data to the same dimension
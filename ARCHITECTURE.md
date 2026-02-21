# Architecture Deep-Dive

## Data Flow

```
[1] NYC Open Data (Socrata API)         full snapshot, ~70k rows
        |
        |  src/extract.py — paged JSON pull, stable sort by license_nbr
        v
[2] data/raw/snapshot_YYYY-MM-DD.csv    immutable raw landing file
        |
        |  src/load_raw.py — DELETE by load_date + COPY (idempotent)
        v
[3] raw.businesses_snapshot             append-only, all TEXT + load_date/ingested_at
        |
        |  dbt: stg_businesses.sql — normalize + cast + dedupe + hash
        v
[4] staging.stg_businesses              clean, typed, one row per (license_nbr, load_date)
        |
        |  dbt snapshot: check strategy on attr_hash
        v
[5] marts.businesses_snapshot           SCD2: dbt_scd_id, valid_from/to, is_current via dbt_valid_to IS NULL
        |
        |  dbt models + singular tests
        v
[6] marts.vw_address_changes            businesses that changed address
    marts.vw_status_history             full status history per business
    marts.quality_summary               run metrics for console report
        |
        v
[7] src/summary.py                      console run report
```

## Stage-by-Stage Reasoning

### Stage 1–2: Extraction → Raw Landing
- **In:** Socrata JSON API, paged by 50,000 rows, sorted by `license_nbr ASC`
- **Why stable sort:** prevents rows from shifting between pages when the dataset is modified mid-pull
- **Out:** `data/raw/snapshot_YYYY-MM-DD.csv`

### Stage 3: Raw Table
- **In:** the CSV
- **Why all TEXT:** absorbs any source schema quirks without crashing — bad dates, unexpected values, trailing spaces all load safely
- **Idempotency:** `DELETE WHERE load_date = :d` before `COPY` ensures re-runs produce the same state
- **Out:** `raw.businesses_snapshot`

### Stage 4: Staging
- **Normalize:** `upper(regexp_replace(trim(col), '\s+', ' ', 'g'))` on all tracked attributes
- **Why normalize before hashing:** "Main St" vs "MAIN  ST" must produce the same hash — otherwise every cosmetic source variation creates a bogus new SCD2 version
- **Dedupe:** `ROW_NUMBER() OVER (PARTITION BY license_nbr, load_date ORDER BY ingested_at DESC)` keeps the latest row if duplicates exist within a single snapshot
- **Hash:** `MD5(name|name2|address|status|category)` — the single value compared for change detection
- **Out:** `staging.stg_businesses`

### Stage 5: SCD2 Dimension
- **Strategy:** dbt `check` on `attr_hash` — only opens a new version when the hash changes
- **Current row:** `dbt_valid_to IS NULL`
- **Key invariants (enforced by tests):**
  - At most one `dbt_valid_to IS NULL` row per `license_nbr`
  - No two versions for the same key have overlapping `dbt_valid_from`/`dbt_valid_to` windows

### Stage 6–7: Analytics + Summary
- Views answer business questions without joining multiple tables
- `summary.py` queries `quality_summary` and prints a human-readable report

## Core Engineering Challenge

**Stage 5 — correct, idempotent SCD2 change detection on dirty data.**

The non-trivial parts are:
1. **Normalize-before-hash:** prevents false-positive versions from cosmetic differences
2. **Idempotency guarantee:** partition-replace raw load + hash comparison = re-running the same snapshot opens zero new versions
3. **Test-enforced invariants:** the build fails if the uniqueness or range guarantees are violated

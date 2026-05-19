CREATE TABLE IF NOT EXISTS raw.businesses_snapshot (
    license_nbr           TEXT,
    business_name         TEXT,
    business_name2        TEXT,
    address_building      TEXT,
    address_street        TEXT,
    address_city          TEXT,
    address_state         TEXT,
    address_zip           TEXT,
    license_status        TEXT,
    license_category      TEXT,
    license_creation_date TEXT,
    load_date             DATE    NOT NULL,
    ingested_at           TIMESTAMP NOT NULL
);

CREATE TABLE IF NOT EXISTS raw.businesses_snapshot_load_audit (
    load_date       DATE PRIMARY KEY,
    source_file_uri TEXT NOT NULL,
    row_count       INTEGER NOT NULL DEFAULT 0,
    rejected_count  INTEGER NOT NULL DEFAULT 0,
    checksum        TEXT NOT NULL,
    status          TEXT NOT NULL,
    loaded_at       TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.businesses_snapshot_dlq (
    id                BIGSERIAL PRIMARY KEY,
    load_date         DATE NOT NULL,
    source_file_uri   TEXT NOT NULL,
    source_row_number INTEGER NOT NULL,
    reject_reason     TEXT NOT NULL,
    raw_payload       TEXT NOT NULL,
    rejected_at       TIMESTAMP NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_businesses_snapshot_load_date
    ON raw.businesses_snapshot (load_date);

CREATE INDEX IF NOT EXISTS idx_businesses_snapshot_dlq_load_date
    ON raw.businesses_snapshot_dlq (load_date);

CREATE TABLE IF NOT EXISTS staging.schema_drift_events (
    id               BIGSERIAL PRIMARY KEY,
    load_date        DATE NOT NULL,
    source_file_uri  TEXT NOT NULL,
    expected_columns TEXT[] NOT NULL,
    observed_columns TEXT[] NOT NULL,
    missing_columns  TEXT[] NOT NULL,
    added_columns    TEXT[] NOT NULL,
    status           TEXT NOT NULL,
    detected_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_schema_drift_events_load_date
    ON staging.schema_drift_events (load_date);

CREATE TABLE IF NOT EXISTS marts.pipeline_run_metrics (
    dag_id           TEXT NOT NULL,
    logical_date     DATE NOT NULL,
    task_id          TEXT NOT NULL,
    try_number       INTEGER NOT NULL DEFAULT 1,
    status           TEXT NOT NULL,
    error_class      TEXT,
    rows_loaded      INTEGER,
    duration_seconds NUMERIC,
    recorded_at      TIMESTAMP NOT NULL DEFAULT now(),
    PRIMARY KEY (dag_id, logical_date, task_id, try_number)
);

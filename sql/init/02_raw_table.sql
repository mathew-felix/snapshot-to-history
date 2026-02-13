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
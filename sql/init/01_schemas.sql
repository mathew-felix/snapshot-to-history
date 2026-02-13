 -- Structure of data pipeline
CREATE SCHEMA IF NOT EXISTS raw;  -- raw data
CREATE SCHEMA IF NOT EXISTS staging; -- cleaned + typed data
CREATE SCHEMA IF NOT EXISTS marts; -- final SCD2 dimension 


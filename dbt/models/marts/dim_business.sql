-- Deterministic SCD2 dimension rebuilt from all staged snapshots.
-- This is intentionally independent from dbt snapshot arrival order so
-- backfills and late-arriving records can repair affected timelines.

with staged as (
    select
        license_nbr,
        business_name,
        business_name2,
        address_full,
        license_status,
        license_category,
        license_creation_date,
        attr_hash,
        load_date
    from {{ ref('stg_businesses') }}
),

ordered as (
    select
        *,
        lag(attr_hash) over (
            partition by license_nbr
            order by load_date
        ) as previous_attr_hash
    from staged
),

change_points as (
    select *
    from ordered
    where previous_attr_hash is null
       or attr_hash != previous_attr_hash
),

effective_ranges as (
    select
        *,
        lead(load_date) over (
            partition by license_nbr
            order by load_date
        ) as valid_to
    from change_points
)

select
    md5(license_nbr || '|' || load_date::text || '|' || attr_hash) as business_sk,
    license_nbr,
    business_name,
    business_name2,
    address_full,
    license_status,
    license_category,
    license_creation_date,
    attr_hash,
    load_date as valid_from,
    valid_to,
    valid_to is null as is_current
from effective_ranges


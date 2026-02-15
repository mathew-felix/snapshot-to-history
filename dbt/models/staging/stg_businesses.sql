-- staging.stg_businesses
-- Normalizes and casts raw text data into clean, typed rows.
-- One row per (license_nbr, load_date). Rejects rows with no license_nbr.
-- Computes attr_hash over tracked attributes for SCD2 change detection.

with raw as (
    select *
    from {{ source('raw', 'businesses_snapshot') }}
    where license_nbr is not null
      and trim(license_nbr) != ''
),

normalized as (
    select
        trim(license_nbr)                                   as license_nbr,

        -- Normalize tracked attributes (trim + upper + collapse whitespace)
        {{ normalize_text('business_name') }}               as business_name,
        {{ normalize_text('business_name2') }}              as business_name2,

        -- Build a single standardized address string
        {{ normalize_text(
            "concat_ws(', ',
                nullif(trim(address_building) || ' ' || trim(address_street), ' '),
                nullif(trim(address_city), ''),
                nullif(trim(address_state), ''),
                nullif(trim(address_zip), '')
            )"
        ) }}                                                as address_full,

        {{ normalize_text('license_status') }}              as license_status,
        {{ normalize_text('license_category') }}            as license_category,

        -- Cast date safely — null if unparseable (PostgreSQL has no try_cast)
        case
            when license_creation_date is not null
                 and trim(license_creation_date) ~ '^\d{4}-\d{2}-\d{2}'
            then left(trim(license_creation_date), 10)::date
            else null
        end                                                 as license_creation_date,

        load_date,
        ingested_at,

        -- Tiebreak: keep the row with the latest ingested_at within a snapshot
        row_number() over (
            partition by trim(license_nbr), load_date
            order by ingested_at desc
        )                                                   as rn

    from raw
),

deduped as (
    select * from normalized where rn = 1
),

hashed as (
    select
        license_nbr,
        business_name,
        business_name2,
        address_full,
        license_status,
        license_category,
        license_creation_date,
        load_date,
        ingested_at,

        -- Change detection hash over all tracked attributes
        md5(
            coalesce(business_name, '')    || '|' ||
            coalesce(business_name2, '')   || '|' ||
            coalesce(address_full, '')     || '|' ||
            coalesce(license_status, '')   || '|' ||
            coalesce(license_category, '')
        )                                               as attr_hash

    from deduped
)

select * from hashed

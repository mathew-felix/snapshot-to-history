-- quality_summary
-- Pipeline run quality metrics — consumed by summary.py for the console report.

select
    (select max(load_date) from {{ source('raw', 'businesses_snapshot') }})
                                                    as snapshot_date,
    (select count(*) from {{ source('raw', 'businesses_snapshot') }}
     where load_date = (select max(load_date) from {{ source('raw', 'businesses_snapshot') }}))
                                                    as raw_rows_ingested,
    (select count(*) from {{ ref('businesses_snapshot') }})
                                                    as total_versions,
    (select count(*) from {{ ref('businesses_snapshot') }} where dbt_valid_to is null)
                                                    as current_rows,
    (select count(distinct license_nbr) from {{ ref('businesses_snapshot') }} where dbt_valid_to is null)
                                                    as unique_current_businesses

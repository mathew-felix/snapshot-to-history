{% snapshot businesses_snapshot %}

{{
    config(
        target_schema='marts',
        unique_key='license_nbr',
        strategy='check',
        check_cols=['attr_hash'],
        invalidate_hard_deletes=False
    )
}}

-- Pull the latest staged snapshot only (most recent load_date)
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
where load_date = (select max(load_date) from {{ ref('stg_businesses') }})

{% endsnapshot %}

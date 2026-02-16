-- vw_address_changes
-- Businesses that changed their address at least once.
-- Shows old address, new address, and when the change occurred.

with versioned as (
    select
        license_nbr,
        business_name,
        address_full,
        dbt_valid_from,
        lag(address_full) over (
            partition by license_nbr
            order by dbt_valid_from
        ) as prev_address
    from {{ ref('businesses_snapshot') }}
)

select
    license_nbr,
    business_name,
    prev_address    as old_address,
    address_full    as new_address,
    dbt_valid_from  as changed_on
from versioned
where prev_address is not null
  and address_full != prev_address

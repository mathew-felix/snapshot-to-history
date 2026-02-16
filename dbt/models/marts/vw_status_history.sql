-- vw_status_history
-- Full history of license status changes per business.

select
    license_nbr,
    business_name,
    license_status,
    dbt_valid_from  as effective_from,
    dbt_valid_to    as effective_to,
    (dbt_valid_to is null) as is_current
from {{ ref('businesses_snapshot') }}
order by license_nbr, dbt_valid_from

-- vw_status_history
-- Full history of license status changes per business.

select
    license_nbr,
    business_name,
    license_status,
    valid_from      as effective_from,
    valid_to        as effective_to,
    is_current
from {{ ref('dim_business') }}
order by license_nbr, valid_from

-- assert_one_current_per_key
-- Fails if any license_nbr has more than one open (current) version.
-- A current row is one where dbt_valid_to IS NULL.

select
    license_nbr,
    count(*) as current_count
from {{ ref('businesses_snapshot') }}
where dbt_valid_to is null
group by license_nbr
having count(*) > 1

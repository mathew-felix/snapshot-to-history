-- assert_one_current_per_key
-- Fails if any license_nbr has more than one open (current) version.
-- A current row is one where is_current is true.

select
    license_nbr,
    count(*) as current_count
from {{ ref('dim_business') }}
where is_current
group by license_nbr
having count(*) > 1

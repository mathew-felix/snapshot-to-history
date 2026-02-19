-- assert_no_overlapping_ranges
-- Fails if any two versions for the same license_nbr have overlapping validity windows.
-- A version A overlaps version B when A.valid_from < B.valid_to AND A.valid_to > B.valid_from.
-- NULL valid_to is treated as 'infinity' (the open/current version).

select
    a.license_nbr,
    a.dbt_scd_id    as version_a,
    b.dbt_scd_id    as version_b,
    a.dbt_valid_from as a_from,
    a.dbt_valid_to   as a_to,
    b.dbt_valid_from as b_from,
    b.dbt_valid_to   as b_to
from {{ ref('businesses_snapshot') }} a
join {{ ref('businesses_snapshot') }} b
    on  a.license_nbr  = b.license_nbr
    and a.dbt_scd_id  != b.dbt_scd_id
    and a.dbt_valid_from < coalesce(b.dbt_valid_to, '9999-12-31'::timestamp)
    and coalesce(a.dbt_valid_to, '9999-12-31'::timestamp) > b.dbt_valid_from

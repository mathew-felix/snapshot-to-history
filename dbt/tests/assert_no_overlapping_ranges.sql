-- assert_no_overlapping_ranges
-- Fails if any two versions for the same license_nbr have overlapping validity windows.
-- A version A overlaps version B when A.valid_from < B.valid_to AND A.valid_to > B.valid_from.
-- NULL valid_to is treated as 'infinity' (the open/current version).

select
    a.license_nbr,
    a.business_sk   as version_a,
    b.business_sk   as version_b,
    a.valid_from    as a_from,
    a.valid_to      as a_to,
    b.valid_from    as b_from,
    b.valid_to      as b_to
from {{ ref('dim_business') }} a
join {{ ref('dim_business') }} b
    on  a.license_nbr  = b.license_nbr
    and a.business_sk != b.business_sk
    and a.valid_from < coalesce(b.valid_to, '9999-12-31'::date)
    and coalesce(a.valid_to, '9999-12-31'::date) > b.valid_from

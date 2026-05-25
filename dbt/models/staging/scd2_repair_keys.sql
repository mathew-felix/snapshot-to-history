-- Keys whose staged history contains older snapshots than the latest loaded date.
-- This model is an operational signal for late-arriving/backfill repair.

with staged as (
    select
        license_nbr,
        load_date
    from {{ ref('stg_businesses') }}
),

max_seen as (
    select
        license_nbr,
        max(load_date) as max_load_date
    from staged
    group by 1
)

select distinct
    staged.license_nbr,
    staged.load_date as late_arriving_load_date,
    max_seen.max_load_date,
    current_timestamp as detected_at
from staged
join max_seen using (license_nbr)
where staged.load_date < max_seen.max_load_date


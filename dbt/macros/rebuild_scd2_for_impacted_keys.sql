{% macro rebuild_scd2_for_impacted_keys(source_relation, key_column, effective_date_column, hash_column) %}
    with ordered as (
        select
            *,
            lag({{ hash_column }}) over (
                partition by {{ key_column }}
                order by {{ effective_date_column }}
            ) as previous_hash
        from {{ source_relation }}
    ),

    change_points as (
        select *
        from ordered
        where previous_hash is null
           or {{ hash_column }} != previous_hash
    )

    select
        *,
        lead({{ effective_date_column }}) over (
            partition by {{ key_column }}
            order by {{ effective_date_column }}
        ) as repaired_valid_to
    from change_points
{% endmacro %}


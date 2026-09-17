{{ config(materialized = 'table') }}

{#
    Both sources in one shape. A month is only included if all its hours
    are present, otherwise the sum is too low and the comparison is
    misleading without showing it. Incomplete months are kept in
    gold.excluded_months rather than disappearing silently.
#}

with per_month as (

    select
        area,
        category,
        month,
        sum(value_mwh)                      as value_mwh,
        count(*)                            as hours_present,
        dayofmonth(last_day(month)) * 24    as hours_expected
    from {{ ref('production_hourly') }}
    group by area, category, month

),

mimer as (

    select
        'mimer' as source,
        area,
        category,
        month,
        value_mwh
    from per_month
    where hours_present = hours_expected

),

scb as (

    select
        'scb' as source,
        area,
        category,
        month,
        value_mwh
    from {{ source('silver_notebook', 'production_monthly') }}
    where measure = 'production'
      and availability = 'reported'

)

select * from mimer
union all
select * from scb

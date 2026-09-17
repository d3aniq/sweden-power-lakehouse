{#
    Aggregation must not lose energy. The sum of the complete months in
    gold has to equal the sum of the same hours in silver.

    Returns a row if it does not.
#}

with silver_total as (

    select sum(value_mwh) as total_mwh
    from (
        select area, category, month, sum(value_mwh) as value_mwh,
               count(*) as hours_present,
               dayofmonth(last_day(month)) * 24 as hours_expected
        from {{ ref('production_hourly') }}
        group by area, category, month
    )
    where hours_present = hours_expected

),

gold_total as (

    select sum(value_mwh) as total_mwh
    from {{ ref('production_monthly') }}
    where source = 'mimer'

)

select
    s.total_mwh as silver_mwh,
    g.total_mwh as gold_mwh,
    s.total_mwh - g.total_mwh as lost_mwh
from silver_total s
cross join gold_total g
where abs(s.total_mwh - g.total_mwh) > 1

{{ config(materialized = 'table') }}

{#
    This cannot be derived from SCB. Wind varies across the day in a
    different way than nuclear, and a monthly figure hides it entirely.
    This model is the justification for fetching hourly data at all.
#}

select
    area,
    category,
    year(ts)                                        as year,
    hour(ts)                                        as hour_of_day,
    round(avg(value_mwh), 1)                        as avg_mwh,
    round(percentile_approx(value_mwh, 0.5), 1)     as median_mwh,
    round(min(value_mwh), 1)                        as min_mwh,
    round(max(value_mwh), 1)                        as max_mwh,
    count(*)                                        as observations
from {{ ref('production_hourly') }}
group by area, category, year(ts), hour(ts)

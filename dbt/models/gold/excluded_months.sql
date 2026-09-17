{{ config(materialized = 'table') }}

{#
    Months where Mimer does not cover every hour. February 2025 is the
    expected one, since Mimer stops publishing on 2025-03-17. Anything
    else here is worth a look before trusting the comparison.
#}

select
    area,
    category,
    month,
    count(*)                            as hours_present,
    dayofmonth(last_day(month)) * 24    as hours_expected
from {{ ref('production_hourly') }}
group by area, category, month
having count(*) != dayofmonth(last_day(month)) * 24

{#
    The production type exists only in Mimer's URL parameter, never in the
    file itself. A wrong code produces no error message but the right name
    on the wrong data, which passes every formal check.

    The test is relative, not absolute. Settled data carries small
    corrections that show up as one or two MWh at night, which is real but
    negligible. A wrong mapping would put night output on the same order as
    noon. Night should be a rounding error next to midday.

    Returns one row per bidding zone where it is not.
#}

with hourly as (

    select
        area,
        hour(ts) as hour_of_day,
        value_mwh
    from {{ ref('production_hourly') }}
    where category = 'solar'

),

night_vs_noon as (

    select
        area,
        avg(case when hour_of_day in (0, 1, 2, 23) then value_mwh end) as night_avg,
        avg(case when hour_of_day = 12 then value_mwh end)             as noon_avg
    from hourly
    group by area

)

select
    area,
    night_avg,
    noon_avg,
    round(night_avg / noon_avg, 4) as night_share
from night_vs_noon
where noon_avg > 0
  and night_avg / noon_avg > 0.02

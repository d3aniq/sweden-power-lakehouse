
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  

with hourly as (

    select
        area,
        hour(ts) as hour_of_day,
        value_mwh
    from `workspace`.`silver`.`production_hourly`
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
  
  
      
    ) dbt_internal_test
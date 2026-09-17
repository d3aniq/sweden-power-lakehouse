
  
    
        create or replace table `workspace`.`gold`.`production_monthly`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      with per_month as (

    select
        area,
        category,
        month,
        sum(value_mwh)                      as value_mwh,
        count(*)                            as hours_present,
        dayofmonth(last_day(month)) * 24    as hours_expected
    from `workspace`.`silver`.`production_hourly`
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
    from `workspace`.`silver`.`production_monthly`
    where measure = 'production'
      and availability = 'reported'

)

select * from mimer
union all
select * from scb
  
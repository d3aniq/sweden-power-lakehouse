
  
    
        create or replace table `workspace`.`gold`.`excluded_months`
      
      
    using delta
  
      
      
      
      
      
      
      
      
      as
      select
    area,
    category,
    month,
    count(*)                            as hours_present,
    dayofmonth(last_day(month)) * 24    as hours_expected
from `workspace`.`silver`.`production_hourly`
group by area, category, month
having count(*) != dayofmonth(last_day(month)) * 24
  
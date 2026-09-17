
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        area, category, ts
    from `workspace`.`silver`.`production_hourly`
    group by area, category, ts
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select hour_of_day
from `workspace`.`gold`.`hourly_profile`
where hour_of_day is null



  
  
      
    ) dbt_internal_test
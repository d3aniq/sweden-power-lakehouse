
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select category
from `workspace`.`silver`.`production_hourly`
where category is null



  
  
      
    ) dbt_internal_test
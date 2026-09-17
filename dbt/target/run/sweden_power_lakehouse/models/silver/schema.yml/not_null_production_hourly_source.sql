
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select source
from `workspace`.`silver`.`production_hourly`
where source is null



  
  
      
    ) dbt_internal_test
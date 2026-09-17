
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select value_mwh
from `workspace`.`silver`.`production_hourly`
where value_mwh is null



  
  
      
    ) dbt_internal_test
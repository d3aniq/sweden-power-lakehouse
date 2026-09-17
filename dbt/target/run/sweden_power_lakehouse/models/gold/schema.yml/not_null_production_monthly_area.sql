
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select area
from `workspace`.`gold`.`production_monthly`
where area is null



  
  
      
    ) dbt_internal_test

    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select category
from `workspace`.`gold`.`production_monthly`
where category is null



  
  
      
    ) dbt_internal_test
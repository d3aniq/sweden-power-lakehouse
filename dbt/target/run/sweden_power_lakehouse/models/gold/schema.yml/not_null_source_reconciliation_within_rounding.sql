
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select within_rounding
from `workspace`.`gold`.`source_reconciliation`
where within_rounding is null



  
  
      
    ) dbt_internal_test
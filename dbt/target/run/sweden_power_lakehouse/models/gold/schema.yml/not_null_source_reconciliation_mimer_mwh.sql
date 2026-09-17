
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select mimer_mwh
from `workspace`.`gold`.`source_reconciliation`
where mimer_mwh is null



  
  
      
    ) dbt_internal_test
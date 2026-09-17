
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select diff_mwh
from `workspace`.`gold`.`source_reconciliation`
where diff_mwh is null



  
  
      
    ) dbt_internal_test
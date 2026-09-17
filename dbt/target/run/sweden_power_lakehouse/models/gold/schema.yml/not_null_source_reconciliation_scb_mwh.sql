
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select scb_mwh
from `workspace`.`gold`.`source_reconciliation`
where scb_mwh is null



  
  
      
    ) dbt_internal_test
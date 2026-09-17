
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  





with validation_errors as (

    select
        area, category, month
    from `workspace`.`gold`.`source_reconciliation`
    group by area, category, month
    having count(*) > 1

)

select *
from validation_errors



  
  
      
    ) dbt_internal_test
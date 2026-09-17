





with validation_errors as (

    select
        area, category, month
    from `workspace`.`gold`.`source_reconciliation`
    group by area, category, month
    having count(*) > 1

)

select *
from validation_errors



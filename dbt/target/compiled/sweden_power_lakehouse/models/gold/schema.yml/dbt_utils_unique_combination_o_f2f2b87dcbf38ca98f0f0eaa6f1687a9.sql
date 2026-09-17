





with validation_errors as (

    select
        source, area, category, month
    from `workspace`.`gold`.`production_monthly`
    group by source, area, category, month
    having count(*) > 1

)

select *
from validation_errors



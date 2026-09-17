





with validation_errors as (

    select
        area, category, ts
    from `workspace`.`silver`.`production_hourly`
    group by area, category, ts
    having count(*) > 1

)

select *
from validation_errors



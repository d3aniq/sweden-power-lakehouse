
    
    

with all_values as (

    select
        source as value_field,
        count(*) as n_records

    from `workspace`.`gold`.`production_monthly`
    group by source

)

select *
from all_values
where value_field not in (
    'mimer','scb'
)



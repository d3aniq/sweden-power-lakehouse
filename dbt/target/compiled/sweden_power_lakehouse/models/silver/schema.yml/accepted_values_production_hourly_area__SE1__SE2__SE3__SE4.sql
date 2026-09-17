
    
    

with all_values as (

    select
        area as value_field,
        count(*) as n_records

    from `workspace`.`silver`.`production_hourly`
    group by area

)

select *
from all_values
where value_field not in (
    'SE1','SE2','SE3','SE4'
)



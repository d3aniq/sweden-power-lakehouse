
    
    

with all_values as (

    select
        measure as value_field,
        count(*) as n_records

    from `workspace`.`silver`.`category_mapping`
    group by measure

)

select *
from all_values
where value_field not in (
    'production','consumption','total','transfer'
)



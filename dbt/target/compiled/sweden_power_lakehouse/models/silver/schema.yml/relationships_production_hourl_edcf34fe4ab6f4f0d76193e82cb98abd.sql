
    
    

with child as (
    select category as from_field
    from `workspace`.`silver`.`production_hourly`
    where category is not null
),

parent as (
    select category as to_field
    from `workspace`.`silver`.`category_mapping`
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null







with comparable_categories as (

    select distinct category
    from `workspace`.`silver`.`category_mapping`
    where comparable

),

wide as (

    select
        area,
        category,
        month,
        max(case when source = 'mimer' then value_mwh end) as mimer_mwh,
        max(case when source = 'scb'   then value_mwh end) as scb_mwh
    from `workspace`.`gold`.`production_monthly`
    where category in (select category from comparable_categories)
    group by area, category, month

)

select
    area,
    category,
    month,
    year(month)                                     as year,
    mimer_mwh,
    scb_mwh,
    mimer_mwh - scb_mwh                             as diff_mwh,
    abs(mimer_mwh - scb_mwh)                        as abs_diff_mwh,
    case
        when scb_mwh != 0
        then round((mimer_mwh - scb_mwh) / scb_mwh * 100, 2)
    end                                             as diff_pct,
    -- Inside SCB's own rounding there is no disagreement to explain.
    abs(mimer_mwh - scb_mwh) <= 500
                                                    as within_rounding
from wide
where mimer_mwh is not null
  and scb_mwh is not null
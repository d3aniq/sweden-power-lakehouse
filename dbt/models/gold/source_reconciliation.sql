{{ config(materialized = 'table') }}

{#
    The point of the project. Two agencies describe the same physical
    reality and the numbers differ. The question is not who is right but
    by how much, where, and why.

    Only categories the mapping marks as comparable are included. Mimer's
    measured unspecified production has no SCB counterpart and would only
    create false deviations.
#}

with comparable_categories as (

    select distinct category
    from {{ ref('category_mapping') }}
    where comparable

),

wide as (

    select
        area,
        category,
        month,
        max(case when source = 'mimer' then value_mwh end) as mimer_mwh,
        max(case when source = 'scb'   then value_mwh end) as scb_mwh
    from {{ ref('production_monthly') }}
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
    abs(mimer_mwh - scb_mwh) <= {{ var('scb_rounding_mwh') }}
                                                    as within_rounding
from wide
where mimer_mwh is not null
  and scb_mwh is not null

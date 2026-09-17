
  
    
        create or replace table `workspace`.`silver`.`production_hourly`
      
      
    using delta
  
      
      partitioned by (area)
      
      
      
      
      
      
      as
      with raw as (

    select *
    from `workspace`.`bronze`.`mimer_production`
    -- The Summa row is the source's own aggregate, not an observation.
    where period_raw != 'Summa'

),

parsed as (

    select
        area,
        production_type,
        to_timestamp(period_raw, 'yyyy-MM-dd HH:mm')        as ts,
        to_timestamp(published_at_raw, 'yyyy-MM-dd HH:mm')  as published_at,
        -- Decimal comma. Read as a thousands separator the value would be
        -- a thousand times too large. See docs/source-differences.md.
        cast(replace(settled_kwh_raw, ',', '.') as decimal(20, 3)) / 1000
                                                            as value_mwh,
        _source_file                                        as source_file
    from raw

),

deduplicated as (

    select
        *,
        row_number() over (
            partition by area, production_type, ts
            order by published_at desc
        ) as version_rank
    from parsed

)

select
    'mimer'                                     as source,
    d.area,
    m.category,
    m.measure,
    d.ts,
    cast(d.ts as date)                          as date,
    cast(date_trunc('month', d.ts) as date)     as month,
    d.value_mwh,
    d.published_at,
    d.source_file,
    current_timestamp()                         as processed_at

from deduplicated d
left join `workspace`.`silver`.`category_mapping` m
    on  d.production_type = m.source_category
    and m.source = 'mimer'
where d.version_rank = 1
  
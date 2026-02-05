{{ config(materialized='table') }}

with distinct_dates as (

    select distinct
        delivery_date as date_day
    from {{ ref('stg_entsoe_day_ahead_prices') }}
    where delivery_date is not null

)

select
    date_day,
    to_number(to_char(date_day, 'YYYYMMDD'))         as date_key,
    year(date_day)                                   as year,
    quarter(date_day)                                as quarter,
    month(date_day)                                  as month,
    to_char(date_day, 'Mon')                         as month_name,
    weekofyear(date_day)                             as week_of_year,
    dayofweek(date_day)                              as day_of_week,
    to_char(date_day, 'DY')                          as day_name,
    iff(dayofweek(date_day) in (0,6), true, false)   as is_weekend
from distinct_dates

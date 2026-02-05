{{ config(materialized='view') }}

with src as (

    select
        load_time,
        filename,
        payload
    from {{ source('entsoe', 'ENTSOE_RAW') }}

),

final as (

    select
        load_time::timestamp_ntz                                   as load_time,
        filename::string                                           as filename,
        payload:"zone"::string                                     as zone,
        payload:"date"::date                                       as delivery_date,
        f.value:"position"::int                                    as position,
        f.value:"price"::float                                     as price_amount
    from src,
    lateral flatten(input => payload:"points") f

)

select * from final
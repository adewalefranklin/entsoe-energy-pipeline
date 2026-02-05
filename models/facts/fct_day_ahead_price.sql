{{ config(materialized='incremental', unique_key='price_key') }}

with src as (

    select
        zone,
        delivery_date,
        position,
        price_amount,
        filename,
        load_time
    from {{ ref('stg_entsoe_day_ahead_prices') }}

    {% if is_incremental() %}
      where load_time > (select coalesce(max(load_time), '1900-01-01') from {{ this }})
    {% endif %}

),

src_dim_date_joined as (

    select
        -- foreign keys
        z.zone_key,
        d.date_key,

        -- natural grain columns
        src.zone                               as zone_code,
        src.delivery_date,
        src.position,
        case
            when src.position between 1  and 6  then 'Early Hours'
            when src.position between 7  and 12 then 'Morning'
            when src.position between 13 and 16 then 'Midday'
            when src.position between 17 and 21 then 'Evening'
            when src.position between 22 and 24 then 'Night'
        else 'Unknown'
        end as period_of_day,

        -- derived timestamp (hour bucket)
        dateadd(hour, src.position - 1, to_timestamp_ntz(src.delivery_date)) as delivery_datetime,

        -- measure
        src.price_amount                       as price_eur_mwh,

        -- lineage
        src.filename,
        src.load_time,

        -- unique key per grain
        {{ dbt_utils.generate_surrogate_key([
            'src.zone',
            "to_varchar(src.delivery_date)",
            'to_varchar(src.position)'
        ]) }} as price_key

    from src
    left join {{ ref('dim_bidding_zone') }} z
      on z.zone_code = src.zone
    left join {{ ref('dim_date') }} d
      on d.date_day = src.delivery_date

)

select * from src_dim_date_joined
where period_of_day != 'Unknown'

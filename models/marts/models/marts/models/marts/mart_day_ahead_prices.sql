{{ config(materialized='view') }}

select
    -- surrogate keys (kept for future relationships and flexibility)
    f.price_key,
    f.zone_key,
    f.date_key,

    -- bidding zone attributes
    z.zone_code,          -- bidding zone short code
    z.zone_full_name,     -- full bidding zone name (stable even if code changes)

    -- date attributes
    d.date_day        as delivery_date,
    d.year,
    d.quarter,
    d.month,
    d.month_name,
    d.week_of_year,
    d.day_name,
    d.is_weekend,

    -- intraday attributes
    f.position,           -- numeric position within the day (can be further bucketed in Power BI if needed)
    f.delivery_datetime,
    f.period_of_day,      -- derived time bucket (Early Hours, Morning, Midday, Evening, Night)

    -- measures
    f.price_eur_mwh,

    -- lineage & audit columns
    f.filename,           -- original source file name
    f.load_time            -- ingestion timestamp from staging

from {{ ref('fct_day_ahead_price') }} f
left join {{ ref('dim_bidding_zone') }} z
    on z.zone_key = f.zone_key
left join {{ ref('dim_date') }} d
    on d.date_key = f.date_key

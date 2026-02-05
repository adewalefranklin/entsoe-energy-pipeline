{{ config(materialized='table') }}

with bidding_zones as (
    select distinct zone as bidding_zone_code
    from {{ ref('stg_entsoe_day_ahead_prices') }}
    where zone is not null
)

select {{ dbt_utils.generate_surrogate_key(['bidding_zone_code']) }} as zone_key,
    bidding_zone_code as zone_code,
    bidding_zone_code as zone_name,
    case when bidding_zone_code = 'AT' then 'Austria'
         when bidding_zone_code = 'BE' then 'Belgium'
         when bidding_zone_code = 'BG' then 'Bulgaria'
         when bidding_zone_code = 'CH' then 'Switzerland'
         when bidding_zone_code = 'CZ' then 'Czech Republic'
         when bidding_zone_code = 'DE-LU' then 'Germany-Luxembourg'
         when bidding_zone_code = 'DK-1' then 'Denmark West'
         when bidding_zone_code = 'DK-2' then 'Denmark East'
         when bidding_zone_code = 'ES' then 'Spain'
         when bidding_zone_code = 'FI' then 'Finland'
         when bidding_zone_code = 'FR' then 'France'
         when bidding_zone_code = 'GB' then 'Great Britain'
         when bidding_zone_code = 'GR' then 'Greece'
         when bidding_zone_code = 'HR' then 'Croatia'
         when bidding_zone_code = 'HU' then 'Hungary'
         when bidding_zone_code = 'IE' then 'Ireland'
         when bidding_zone_code = 'IT1' then 'Italy North'
         when bidding_zone_code = 'IT2' then 'Italy Central South'
         when bidding_zone_code = 'IT3' then 'Italy South'
         when bidding_zone_code = 'LT' then 'Lithuania'
         when bidding_zone_code = 'LV' then 'Latvia'
         when bidding_zone_code = 'NL' then 'Netherlands'
         when bidding_zone_code = 'NO1' then 'Norway NO1 (Oslo)'
         when bidding_zone_code = 'NO2' then 'Norway NO2 (Kristiansand)'
         when bidding_zone_code = 'NO3' then 'Norway NO3 (Trondheim)'
         when bidding_zone_code = 'NO4' then 'Norway NO4 (Tromsø)'
         when bidding_zone_code = 'PL' then 'Poland'
         when bidding_zone_code = 'PT' then 'Portugal'
         when bidding_zone_code = 'RO' then 'Romania'
         when bidding_zone_code = 'SE1' then 'Sweden SE1 (Luleå)'
         when bidding_zone_code = 'SE2' then 'Sweden SE2 (Sundsvall)'
         when bidding_zone_code = 'SE3' then 'Sweden SE3 (Stockholm)'
         when bidding_zone_code = 'SE4' then 'Sweden SE4 (Malmö)'
         when bidding_zone_code = 'SI' then 'Slovenia'
         when bidding_zone_code = 'SK' then 'Slovakia'
         else 'Unknown' 
         end as zone_full_name
    from bidding_zones
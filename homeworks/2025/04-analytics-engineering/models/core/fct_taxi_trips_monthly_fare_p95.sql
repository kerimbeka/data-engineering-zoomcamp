{{
    config(
        materialized='table'
    )
}}

with filtered_data as (
    select
        service_type,
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        fare_amount
    from {{ ref('fct_taxi_trips') }}
    where fare_amount > 0
    and trip_distance > 0
    and payment_type_description in ('Cash', 'Credit Card')
)
select 
    service_type,
    year,
    month,
    PERCENTILE_CONT(0.97) WITHIN GROUP (ORDER BY fare_amount) as p97,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY fare_amount) as p95,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY fare_amount) as p90
from filtered_data
GROUP BY service_type, year, month
ORDER BY year, month, service_type

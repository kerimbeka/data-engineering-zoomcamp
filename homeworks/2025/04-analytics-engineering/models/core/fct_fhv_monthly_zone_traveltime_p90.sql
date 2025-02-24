{{
    config(
        materialized='table'
    )
}}

with fhv_trips as (
    select *,
    EXTRACT(EPOCH FROM (dropoff_datetime - pickup_datetime)) as trip_duration,
    EXTRACT(year from pickup_datetime) as fhv_year,
    EXTRACT(month from pickup_datetime) as fhv_month
    from {{ ref('dim_fhv_trips') }}
),
percentiles as (
    select 
        year, 
        month, 
        pickup_locationid, 
        dropoff_locationid,
        percentile_cont(0.90) within group (order by trip_duration) as p90_trip_duration
    from fhv_trips
    group by year, month, pickup_locationid, dropoff_locationid
)

select *
from percentiles

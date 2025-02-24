{{
    config(
        materialized='table'
    )
}}

with trips_aggregated as (
    select 
        extract(year from pickup_datetime) as year,
        extract(quarter from pickup_datetime) as quarter,
        service_type, 
        sum(total_amount) as revenue_quarterly_total_amount
    from {{ ref('fct_taxi_trips') }}
    group by 1,2,3
)
select
    year,
    quarter,
    service_type,
    revenue_quarterly_total_amount,
    lag(revenue_quarterly_total_amount) over (
        partition by service_type, quarter 
        order by year
    ) as prev_year_quarterly_revenue,
    case 
        when lag(revenue_quarterly_total_amount) over (
            partition by service_type, quarter 
            order by year
        ) = 0 then null
        else (revenue_quarterly_total_amount - lag(revenue_quarterly_total_amount) over (
            partition by service_type, quarter 
            order by year
        )) / nullif(lag(revenue_quarterly_total_amount) over (
            partition by service_type, quarter 
            order by year
        ), 0) * 100 
    end as yoy_growth
from trips_aggregated

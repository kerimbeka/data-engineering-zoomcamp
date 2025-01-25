{{ config(materialized="view") }}

with tripdata as (select * from {{ source("staging", "fhv_tripdata") }})
select
    -- identifiers   
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }}
    as dispatching_base_num,
    {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("string")) }}
    as affiliated_base_number,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }}
    as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }}
    as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }}
    as shared_trips_flag,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime

from tripdata

SELECT COUNT(*) FROM `warm-utility-413915.dbt_bkerimkulov.fact_fhv_trips`

SELECT COUNT(1) FROM `warm-utility-413915.dbt_bkerimkulov.fact_trips`
WHERE pickup_datetime between '2019-07-01' and '2019-07-31' and dropoff_datetime between '2019-07-01' and '2019-07-31' and service_type = 'Green'

SELECT COUNT(1) FROM `warm-utility-413915.dbt_bkerimkulov.fact_trips`
WHERE pickup_datetime between '2019-07-01' and '2019-07-31' and dropoff_datetime between '2019-07-01' and '2019-07-31' and service_type = 'Yellow'
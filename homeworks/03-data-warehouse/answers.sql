-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `warm-utility-413915.ny_taxi.external_green_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-bucket-bekzhan/green/green_tripdata_2022.parquet']
);

-- Creating a table in BQ
CREATE OR REPLACE TABLE `warm-utility-413915.ny_taxi.green_tripdata_nonpartitioned`
AS SELECT * FROM `warm-utility-413915.ny_taxi.external_green_tripdata`;


-- Question 1
SELECT count(*) FROM `warm-utility-413915.ny_taxi.external_green_tripdata`;

-- Question 2
SELECT COUNT(DISTINCT(PULocationID)) FROM `warm-utility-413915.ny_taxi.external_green_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `warm-utility-413915.ny_taxi.green_tripdata_nonpartitioned`;

-- Question 3
SELECT COUNT(*) FROM `warm-utility-413915.ny_taxi.external_green_tripdata`
WHERE fare_amount = 0;

-- Question 4
-- Partition by lpep_pickup_datetime Cluster on PUlocationID

CREATE OR REPLACE TABLE `warm-utility-413915.ny_taxi.green_tripdata_partitioned`
PARTITION BY DATE(TIMESTAMP_MICROS(CAST(lpep_pickup_datetime/1000 AS INT64)))
CLUSTER BY PUlocationID AS (
  SELECT * FROM `warm-utility-413915.ny_taxi.external_green_tripdata`
);

-- Question 5
-- This query will process 12.82 MB when run
SELECT COUNT(DISTINCT(PULocationID)) FROM  `warm-utility-413915.ny_taxi.green_tripdata_nonpartitioned`
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

-- This query will process 1.12 MB when run.
SELECT COUNT(DISTINCT(PULocationID)) FROM  `warm-utility-413915.ny_taxi.green_tripdata_partitioned`
WHERE DATE(lpep_pickup_datetime_timestamp) BETWEEN '2022-06-01' AND '2022-06-30';
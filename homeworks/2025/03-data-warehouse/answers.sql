-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `terraform-demo-412122.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'parquet',
  uris = ['gs://de-zoomcamp-bucket-bekzhan/yellow-taxi/yellow_tripdata_2024-01.parquet',
          'gs://de-zoomcamp-bucket-bekzhan/yellow-taxi/yellow_tripdata_2024-02.parquet',
          'gs://de-zoomcamp-bucket-bekzhan/yellow-taxi/yellow_tripdata_2024-03.parquet',
          'gs://de-zoomcamp-bucket-bekzhan/yellow-taxi/yellow_tripdata_2024-04.parquet',
          'gs://de-zoomcamp-bucket-bekzhan/yellow-taxi/yellow_tripdata_2024-05.parquet',
          'gs://de-zoomcamp-bucket-bekzhan/yellow-taxi/yellow_tripdata_2024-06.parquet']
);

-- Creating a table in BQ
CREATE OR REPLACE TABLE `terraform-demo-412122.ny_taxi.yellow_tripdata_nonpartitioned`
AS SELECT * FROM `terraform-demo-412122.ny_taxi.external_yellow_tripdata`;


-- Question 1
SELECT COUNT(*) FROM `terraform-demo-412122.ny_taxi.external_yellow_tripdata`;

-- Question 2
SELECT COUNT(DISTINCT(PULocationID)) FROM `terraform-demo-412122.ny_taxi.external_yellow_tripdata`;

SELECT COUNT(DISTINCT(PULocationID)) FROM `terraform-demo-412122.ny_taxi.yellow_tripdata_nonpartitioned`;

-- Question 3
SELECT PULocationID FROM `terraform-demo-412122.ny_taxi.yellow_tripdata_nonpartitioned`;

SELECT PULocationID, DOLocationID FROM `terraform-demo-412122.ny_taxi.yellow_tripdata_nonpartitioned`

-- Question 4
SELECT COUNT(*) FROM `terraform-demo-412122.ny_taxi.external_yellow_tripdata`
WHERE fare_amount = 0;

-- Question 5
CREATE OR REPLACE TABLE `terraform-demo-412122.ny_taxi.yellow_tripdata_partitioned`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS (
  SELECT * FROM `terraform-demo-412122.ny_taxi.external_yellow_tripdata`
);

-- Question 6
SELECT COUNT(DISTINCT(VendorID)) FROM  `terraform-demo-412122.ny_taxi.yellow_tripdata_nonpartitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';

SELECT COUNT(DISTINCT(VendorID)) FROM  `terraform-demo-412122.ny_taxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_dropoff_datetime) BETWEEN '2024-03-01' AND '2024-03-15';
1. 128.3 MB
2. "green_tripdata_2020-04.csv"
3. 24648219
4. 1733998
5. 1925152
6. Add a timezone property set to America/New_York in the Schedule trigger configuration

Solution:
1. Run backfill and look at Outputs->extract->outputFiles
2. Run backfill and look at Outputs->extract->outputFiles
3. 
```{sql}
SELECT COUNT(*) AS total_rows
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= '2020-01-01'
AND tpep_pickup_datetime < '2021-01-01';
```
4. 
```{sql}
SELECT COUNT(*) AS total_rows
FROM green_tripdata
WHERE lpep_pickup_datetime >= '2020-01-01'
AND lpep_pickup_datetime < '2021-01-01';
```
5.
```{sql}
SELECT COUNT(*) AS total_rows
FROM yellow_tripdata
WHERE tpep_pickup_datetime >= '2021-03-01'
AND tpep_pickup_datetime < '2021-04-01';
```

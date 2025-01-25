## Answers

1. 24.3.1
2. db:5432
3. 104802, 198924, 109603, 27678, 35189
4. 2019-10-31
5. East Harlem North, East Harlem South, Morningside Heights
6. JFK Airport
7. terraform init, terraform apply -auto-approve, terraform destroy


## Solution
1. 
```{bash}
docker run -it --entrypoint bash python:3.12.8
pip --version
```

3.
```{sql}
SELECT 
    SUM(CASE WHEN trip_distance <= 1 THEN 1 ELSE 0 END) AS "Up to 1 mile",
    SUM(CASE WHEN trip_distance > 1 AND trip_distance <= 3 THEN 1 ELSE 0 END) AS "Between 1 and 3 miles",
    SUM(CASE WHEN trip_distance > 3 AND trip_distance <= 7 THEN 1 ELSE 0 END) AS "Between 3 and 7 miles",
    SUM(CASE WHEN trip_distance > 7 AND trip_distance <= 10 THEN 1 ELSE 0 END) AS "Between 7 and 10 miles",
    SUM(CASE WHEN trip_distance > 10 THEN 1 ELSE 0 END) AS "Over 10 miles"
FROM 
    green_taxi_data
WHERE 
    lpep_pickup_datetime >= '2019-10-01' AND lpep_dropoff_datetime < '2019-11-01';
```

4.
```{sql}
SELECT 
	DATE(lpep_pickup_datetime) AS pickup_date,
	MAX(trip_distance) AS max_trip_distance
FROM 
	green_taxi_data
GROUP BY 
	DATE(lpep_pickup_datetime)
ORDER BY 
    max_trip_distance DESC
```
5.
```{sql}
SELECT 
    "z"."Zone" AS pickup_location_name,
    SUM(g.total_amount) AS total_amount
FROM 
    green_taxi_data g
JOIN 
    zones z ON "g"."PULocationID" = "z"."LocationID"
WHERE 
    DATE(g.lpep_pickup_datetime) = '2019-10-18'
GROUP BY 
    "z"."Zone"
HAVING 
    SUM(g.total_amount) > 13000
ORDER BY 
    total_amount DESC;
```
6.
```{sql}
SELECT 
    "z2"."Zone" AS dropoff_zone_name,
    MAX(g.tip_amount) AS largest_tip
FROM 
    green_taxi_data g
JOIN 
    zones z1 ON "g"."PULocationID" = "z1"."LocationID"
JOIN 
    zones z2 ON "g"."DOLocationID" = "z2"."LocationID"
WHERE 
    "z1"."Zone" = 'East Harlem North' AND
    DATE(g.lpep_pickup_datetime) BETWEEN '2019-10-01' AND '2019-10-31'
GROUP BY 
    "z2"."Zone"
ORDER BY 
    largest_tip DESC
LIMIT 1;
```

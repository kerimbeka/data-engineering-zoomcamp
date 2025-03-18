```{bash}
uv run pgcli -h localhost -p 5432 -u postgres -d postgres
```
```{sql}
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER
);
```
## Question 1.
```{bash}
docker exec -it redpanda-1 bash
```
```{bash}
rpk version
 ```
Answer:
```{bash}
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```
## Question 2
```{bash}
rpk topic create green-trips
```
```{bash}
TOPIC        STATUS
green-trips  OK
```

## Question 3
```{bash}
uv run python send_data.py
```
True

## Question 4
```{bash}
uv run python send_data.py
```
54.81 seconds

## Question 5
```{bash}
docker exec -it flink-jobmanager ./bin/flink run -py ./../src/job/session_job.py -d
```
```{sql}
SELECT *
FROM green_trips_aggregated
WHERE taxi_trips_streak = (
    SELECT MAX(taxi_trips_streak)
    FROM green_trips_aggregated
);
```
+--------------+--------------+-------------------+
| pulocationid | dolocationid | taxi_trips_streak |
|--------------+--------------+-------------------|
| 95           | 95           | 44                |
+--------------+--------------+-------------------+
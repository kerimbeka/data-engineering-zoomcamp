from time import time
from pathlib import Path

import polars as pl
import json

from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


server = "localhost:9092"

producer = KafkaProducer(bootstrap_servers=[server], value_serializer=json_serializer)

print(producer.bootstrap_connected())

cols = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "tip_amount",
]

df = pl.read_csv(Path(__file__).parent / "data" / "green_tripdata_2019-10.csv")
df = df.select(pl.col(cols))

print(df.head())

t0 = time()

for row in df.iter_rows(named=True):
    producer.send("green-trips", value=row)

producer.flush()

t1 = time()
took = t1 - t0

print(f"Time taken to send and flush: {took:.2f} seconds")

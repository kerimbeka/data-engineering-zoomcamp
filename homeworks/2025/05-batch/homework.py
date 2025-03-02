import os
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_date
from pyspark.sql.functions import unix_timestamp
from pyspark.sql import types

spark = SparkSession.builder \
    .appName('test') \
    .getOrCreate()


print("Question 1: ", spark.version)

df = spark.read.option("header", "true").parquet("./data/yellow_tripdata_2024-10.parquet")

df = df.repartition(4)

#df.write.csv("./data/yellow/2024/10")

df = df.withColumn('pickup_date', to_date('tpep_pickup_datetime'))
oct_15_trips = df.filter(col("pickup_date") == "2024-10-15")

# Count the number of taxi trips on October 15th
trip_count_oct_15 = oct_15_trips.count()
print("Question 3: ", trip_count_oct_15)

df_with_duration = df.withColumn(
    'duration_hours',
    (unix_timestamp('tpep_dropoff_datetime') - unix_timestamp('tpep_pickup_datetime')) / 3600
)

print("Question 4: ",
    df_with_duration.agg({"duration_hours": "max"}).collect()
    )

schema_zones = types.StructType([
    types.StructField('LocationID', types.IntegerType(), True),
    types.StructField('Borough', types.StringType(), True),
    types.StructField('Zone', types.StringType(), True),
    types.StructField('service_zone', types.StringType(), True)
])

spark_ui_port = spark.sparkContext.uiWebUrl
print("Question 5:", spark_ui_port)

df_zones = spark.read \
    .option("header", "true") \
    .schema(schema_zones) \
    .csv('./data/taxi_zone_lookup.csv')
    
print("Question 6: ",
    df.join(df_zones, df["PULocationID"] == df_zones["LocationID"])
    .groupBy("Zone")
    .count()
    .orderBy(col("count"))
    .first()["Zone"]
)
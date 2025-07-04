import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JoinUberZones") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/uber_analysis_sample.parquet")

df_zones = spark.read.csv("data/external/taxi+_zone_lookup.csv", header=True)

# Pickup
df = df.join(
    df_zones.withColumnRenamed("LocationID", "PULocationID") \
        .withColumnRenamed("Borough", "PUBorough") \
        .withColumnRenamed("Zone", "PUZone"),
    on="PULocationID",
    how="left"
)

# Dropoff
df = df.join(
    df_zones.withColumnRenamed("LocationID", "DOLocationID") \
        .withColumnRenamed("Borough", "DOBorough") \
        .withColumnRenamed("Zone", "DOZone"),
    on="DOLocationID",
    how="left"
)

# Preview
df_sample = df.select(
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "base_passenger_fare",
    "PUBorough",
    "PUZone",
    "DOBorough",
    "DOZone"
)

df_sample.show(5, truncate=False)

df_sample.write.parquet("data/cleaned/uber_joined_zones.parquet", mode="overwrite")

spark.stop()
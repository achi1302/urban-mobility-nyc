import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("JoinTaxiZones") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/yellowtaxi_analysis_sample.parquet")

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
filtered_df = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "payment_type",
    "PUBorough",
    "PUZone",
    "DOBorough",
    "DOZone"
)

filtered_df.show(10, truncate=False)

# Save
filtered_df.write.parquet("data/cleaned/yellowtaxi_joined_zones.parquet", mode="overwrite")

spark.stop()


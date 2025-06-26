import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, lit, sum, round

spark = SparkSession.builder \
    .appName("MobilityByBorough") \
    .getOrCreate()

# Load Taxi
taxi_df = spark.read.parquet("data/cleaned/taxi/yellowtaxi_final.parquet") \
    .withColumn("provider", lit("taxi"))

# Load and Reformat Uber
uber_df = spark.read.parquet("data/cleaned/uber/uber_final.parquet") \
    .withColumn("provider", lit("uber")) \
    .withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
    .withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime") \
    .withColumnRenamed("trip_miles", "trip_distance") \
    .withColumnRenamed("base_passenger_fare", "fare_amount") \
    .withColumn("passenger_count", lit(None).cast("long")) \
    .withColumn("payment_type", lit(None).cast("string")) \
    
columns = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "payment_type",
    "PUBorough", "PUZone", "DOBorough", "DOZone", "provider"
]

df_union = taxi_df.select(columns).unionByName(uber_df.select(columns))

# Aggregate by Pickup and Provider
aggregated_df = df_union.groupBy("PUBorough", "provider").agg(
    count("*").alias("trip_count"),
    avg("trip_distance").alias("avg_trip_distance"),
    avg("fare_amount").alias("avg_fare_amount")
)  

# Remove unwanted boroughs
remove_boroughs = ["Unknown", "N/A", "EWR"]
aggregated_df = aggregated_df.filter(~col("PUBorough").isin(remove_boroughs))

aggregated_df.show(truncate=False)

aggregated_df.groupBy("provider").agg(
    sum("trip_count").alias("total_trips")
).show(truncate=False)

aggregated_df = aggregated_df.withColumn(
    "fare_per_mile",
    round(col("avg_fare_amount") / col("avg_trip_distance"), 2)
)

aggregated_df.show(truncate=False)

spark.stop()


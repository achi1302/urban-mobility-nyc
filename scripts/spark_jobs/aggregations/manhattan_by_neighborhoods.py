import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, sum, round

spark = SparkSession.builder \
    .appName("ManhattanByNeighborhoods") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/manhattan_trips.parquet")

# Aggregate by PickupZone and Provider
aggregated_df = df.groupBy("PURegion", "PUZone", "provider").agg(
    count("*").alias("trip_count"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
) 

# Trip Differential
aggregated_df = aggregated_df.withColumn(
    "fare_per_mile",
    round(col("avg_fare_amount") / col("avg_trip_distance"), 2)
)

region_summary = df.groupBy("PURegion","Provider").agg(
    count("*").alias("total_trips"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
).withColumn(
    "fare_per_mile",
    round(col("avg_fare_amount") / col("avg_trip_distance"), 2)
)

aggregated_df.orderBy("PURegion", "PUZone", "provider").show(50, truncate=False)

region_summary.orderBy("PURegion", "Provider").show(truncate=False)

spark.stop()
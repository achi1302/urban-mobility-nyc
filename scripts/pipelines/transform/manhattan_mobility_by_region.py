import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round, first

spark = SparkSession.builder \
    .appName("ManhattanByRegions") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/manhattan_trips.parquet")

# Aggregate by PickupZone and Provider
aggregated_df = df.groupBy("PURegion", "PUZone", "provider").agg(
    count("*").alias("trip_count"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
).withColumn( #FarePerMile
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

pivot_region_summary = region_summary.groupBy("PURegion").pivot("Provider").agg(
    first("total_trips").alias("total_trips"),
    first("avg_trip_distance").alias("avg_trip_distance"),
    first("avg_fare_amount").alias("avg_fare_amount"),
    first("fare_per_mile").alias("fare_per_mile")
)

rename = pivot_region_summary.select(
    col("PURegion"),
    col("uber_total_trips").alias("total_trips_uber"),
    col("taxi_total_trips").alias("total_trips_taxi")
    
)

trip_diferential = rename.withColumn(
    "trip_differential",
    col("total_trips_uber") - col("total_trips_taxi")
)

census_df = spark.read.csv("data/cleaned/manhattan_region_census.csv", header=True, inferSchema=True)

region_summary_joined = region_summary.join(census_df, on="PURegion", how="left")


aggregated_df.orderBy("PURegion", "PUZone", "provider").show(10, truncate=False)

trip_diferential.orderBy("PURegion").show(truncate=False)

region_summary_joined.orderBy("PURegion", "Provider").show(truncate=False)

region_summary_joined.coalesce(1).write.option("header", True).mode("overwrite").csv("data/outputs/manhattan_mobility_by_region_csv")
region_summary_joined.write.mode("overwrite").parquet("data/outputs/manhattan_mobility_by_region.parquet")

spark.stop()
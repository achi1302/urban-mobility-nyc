import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, round, date_format, hour

# Increased Memory
spark = SparkSession.builder \
    .appName("AggregatAndFilterTaxiUberData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" #Change Year

df = spark.read.parquet(f"data/cleaned/{YEAR}/manhattan_taxi_uber_tripdata_{YEAR}.parquet")
census_df = spark.read.csv(f"data/cleaned/{YEAR}/manhattan_region_census_{YEAR}.csv", header=True, inferSchema=True)

# AGGREGATE AND FILTER
df_zone_summary = df.groupBy("PURegion", "PUZone", "provider").agg(
    count("*").alias("trip_count"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
).withColumn( #FarePerMile
    "fare_per_mile",
    round(col("avg_fare_amount") / col("avg_trip_distance"), 2)
)

df_region_summary = df.groupBy("PURegion", "provider").agg(
    count("*").alias("trip_count"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance")
).withColumn(
    "fare_per_mile",
    round(col("avg_fare_amount") / col("avg_trip_distance"), 2)
)
df_region_summary = df_region_summary.join(census_df, on="PURegion", how="left")

df_monthly_region_trends = (
    df.withColumn("year_month", date_format("tpep_pickup_datetime", "yyyy-MM"))
        .groupBy("year_month", "PURegion", "provider")
        .agg(
            count("*").alias("trip_count"),
            round(avg("trip_distance"), 2).alias("avg_trip_distance"),
            round(avg("fare_amount"), 2).alias("avg_fare_amount")
        )
        .withColumn("fare_per_mile", round(col("avg_fare_amount") / col("avg_trip_distance"), 2))
)

df_provider_summary = df.groupBy("provider").agg(
    count("*").alias("trip_count"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount"),
    round(avg("fare_amount") / avg("trip_distance"), 2).alias("fare_per_mile")
)

df_zone_flow = df.groupBy("PUZone", "DOZone").agg(
    count("*").alias("trip_count"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
)

df_payment_type_summary = df.groupBy("payment_type").agg(
    count("*").alias("trip_count"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
)

df_hourly_trends = df.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour", "provider") \
    .agg(
        count("*").alias("trip_count"),
        round(avg("fare_amount"), 2).alias("avg_fare_amount")
    )

print("\n Manhattan:")
df_rows = df.count()
print(df_rows)
df.show(10, truncate=False)

print("\n Zone Summary:")
df_zone_summary.orderBy("PURegion", "PUZone", "provider").show(10, truncate=False)

print("\n Regions Summary:")
df_region_summary.orderBy("PURegion", "provider").show(truncate=False)

print("\n Monthly Regional Trends:")
df_monthly_region_trends.orderBy("year_month", "provider").show(10, truncate=False)

print("\n Hourly Trends:")
df_hourly_trends.show(10, truncate=False)

print("\n Provider Summary:")
df_provider_summary.show(10, truncate=False)

print("\n Flow Zone:")
df_zone_flow_rows = df_zone_flow.count()
print(df_zone_flow_rows)
df_zone_flow.show(10, truncate=False)

print("\n Payment Type Summary:")
df_payment_type_summary.show(truncate=False)

##CENSUS TABLES




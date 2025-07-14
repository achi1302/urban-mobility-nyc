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
OUTPUT_PATH = f"data/outputs/{YEAR}"

df = spark.read.parquet(f"data/cleaned/{YEAR}/manhattan_taxi_uber_tripdata_{YEAR}.parquet")
census_df = spark.read.csv(f"data/cleaned/{YEAR}/manhattan_region_census_{YEAR}.csv", header=True, inferSchema=True)

# BI READY TABLES
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
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
).withColumn(
    "fare_per_mile",
    round(col("avg_fare_amount") / col("avg_trip_distance"), 2)
)
df_region_summary = df_region_summary.join(census_df, on="PURegion", how="left")

df_monthly_region_trends = df.withColumn("year_month", date_format("tpep_pickup_datetime", "yyyy-MM")) \
    .groupBy("year_month", "PURegion", "provider") \
    .agg(
        count("*").alias("trip_count"),
        round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        round(avg("fare_amount"), 2).alias("avg_fare_amount")
    ) \
    .withColumn("fare_per_mile", round(col("avg_fare_amount") / col("avg_trip_distance"), 2))
df_monthly_region_trends = df_monthly_region_trends.join(census_df, on="PURegion", how="left")

df_hourly_trends = df.withColumn("pickup_hour", hour("tpep_pickup_datetime")) \
    .groupBy("pickup_hour", "provider") \
    .agg(
        count("*").alias("trip_count"),
        round(avg("fare_amount"), 2).alias("avg_fare_amount")
    )

df_provider_summary = df.groupBy("provider").agg(
    count("*").alias("trip_count"),
    round(avg("trip_distance"), 2).alias("avg_trip_distance"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount"),
    round(avg("fare_amount") / avg("trip_distance"), 2).alias("fare_per_mile")
)

df_provider_pref = df.groupBy("PURegion", "provider").agg(
    count("*").alias("trip_count")
)
df_provider_pref = df_provider_pref.join(census_df, on="PURegion", how="left")

df_payment_type_summary = df.groupBy("payment_type").agg(
    count("*").alias("trip_count"),
    round(avg("fare_amount"), 2).alias("avg_fare_amount")
)

df_fare_per_mile = df.groupBy("PURegion", "provider").agg(
    round(avg("fare_amount") / avg("trip_distance"), 2).alias("fare_per_mile")
)
df_fare_per_mile = df_fare_per_mile.join(census_df, on="PURegion", how="left")

df_density_trips = df.groupBy("PURegion").agg(
    count("*").alias("trip_count")
)
df_density_trips = df_density_trips.join(census_df, on="PURegion", how="left")

#LOAD AND PREVIEW
print("\n Manhattan:")
df_rows = df.count()
print(df_rows)
df.show(10, truncate=False)

print("\n Zone Summary:")
df_zone_summary.orderBy("PURegion", "PUZone", "provider").show(10, truncate=False)
df_zone_summary.write.parquet(f"{OUTPUT_PATH}/manhattan_zone_summary_{YEAR}.parquet", mode="overwrite")
print("Manhattan Zone Summary Loaded!")

print("\n Regions Summary:")
df_region_summary.orderBy("PURegion", "provider").show(truncate=False)
df_region_summary.write.parquet(f"{OUTPUT_PATH}/manhattan_region_summary_{YEAR}.parquet", mode="overwrite")
print("Manhattan Region Summary Loaded!")

print("\n Monthly Regional Trends:")
df_monthly_region_trends.orderBy("year_month", "provider").show(10, truncate=False)
df_monthly_region_trends.write.parquet(f"{OUTPUT_PATH}/manhattan_monthly_region_trends_{YEAR}.parquet", mode="overwrite")
print("Manhattan Monthly Region Trends Loaded!")

print("\n Hourly Trends:")
df_hourly_trends.show(10, truncate=False)
df_hourly_trends.write.parquet(f"{OUTPUT_PATH}/manhattan_hourly_trends_{YEAR}.parquet", mode="overwrite")
print("Manhattan Hourly Tredns Loaded!")

print("\n Provider Summary:")
df_provider_summary.show(truncate=False)
df_provider_summary.write.parquet(f"{OUTPUT_PATH}/manhattan_provider_summary_{YEAR}.parquet", mode="overwrite")
print("Manhattan Povider Summary Loaded!")

print("\n Provider Preference by Wealth:")
df_provider_pref.orderBy("PURegion", "provider").show(truncate=False)
df_provider_pref.write.parquet(f"{OUTPUT_PATH}/manhattan_provider_preference_by_wealth_{YEAR}.parquet", mode="overwrite")
print("Manhattan Provider Preference by Wealth Loaded!")

print("\n Payment Type Summary:")
df_payment_type_summary.show(truncate=False)
df_payment_type_summary.write.parquet(f"{OUTPUT_PATH}/manhattan_payment_type_{YEAR}.parquet", mode="overwrite")
print("Manhattan Payment Type Loaded!")

print("\n Fare per Mile vs. Property Value:")
df_fare_per_mile.orderBy("PURegion", "provider").show(truncate=False)
df_fare_per_mile.write.parquet(f"{OUTPUT_PATH}/manhattan_fare_per_mile_vs_property_value_{YEAR}.parquet", mode="overwrite")
print("Manhattan Fare per Mile vs. Property Value Loaded!")

print("Population Density vs. Trip Volume:")
df_density_trips.show(truncate=False)
df_density_trips.write.parquet(f"{OUTPUT_PATH}/manhattan_population_density_vs_trip_volume_{YEAR}.parquet", mode="overwrite")
print("Manhattan Population Density vs. Trip Volume Loaded!")
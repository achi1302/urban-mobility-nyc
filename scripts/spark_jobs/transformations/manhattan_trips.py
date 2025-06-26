import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

spark = SparkSession.builder \
    .appName("ManhattanTrips") \
    .getOrCreate()

# Load Data
taxi_df = spark.read.parquet("data/cleaned/taxi/yellowtaxi_final.parquet") \
    .withColumn("provider", lit("taxi"))

# Reformat Uber
uber_df = spark.read.parquet("data/cleaned/uber/uber_final.parquet") \
    .withColumn("provider", lit("uber")) \
    .withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
    .withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime") \
    .withColumnRenamed("trip_miles", "trip_distance") \
    .withColumnRenamed("base_passenger_fare", "fare_amount") \
    .withColumn("passenger_count", lit(None).cast("long")) \
    .withColumn("payment_type", lit(None).cast("string")) \

target_neighborhoods = [
    "Upper East Side North", "Upper East Side South",
    "Upper West Side North", "Upper West Side South",
    "Battery Park City", "Tribeca/Civic Center",
    "SoHo", "West Village", "East Harlem South", "East Harlem North",
    "Central Harlem", "Yorkville East", "Yorkville West",
    "Midtown Center", "Midtown North",
    "Penn Station/Madison Sq", "Times Sq/Theatre District",
    "Inwood", "Washington Heights North", "Washington Heights South",
    "Hamilton Heights", "Morningside Heights", "Clinton East",
    "Clinton West", "Financial District North", "Financial District South"
]

columns = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "payment_type",
    "PUBorough", "PUZone", "DOBorough", "DOZone", "provider"
]

df = taxi_df.select(columns).unionByName(uber_df.select(columns))

df = df.filter(
    (col("PUBorough") == "Manhattan") &
    (col("DOBorough") == "Manhattan") &
    (col("PUZone").isin(target_neighborhoods)) &
    (col("DOZone").isin(target_neighborhoods))
)

df.select("tpep_pickup_datetime", "PUZone", "DOZone", "provider", "fare_amount", "trip_distance").show(10, truncate=False)

df.write.parquet("data/cleaned/manhattan_trips.parquet", mode="overwrite")

spark.stop()



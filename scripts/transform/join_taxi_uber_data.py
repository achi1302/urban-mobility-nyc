import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Increased Memory
spark = SparkSession.builder \
    .appName("JoinTaxiUberData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" #Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/taxi_uber_joined_tripdata_{YEAR}.parquet"

# Taxi
taxi_df = spark.read.parquet(f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_ultimate_{YEAR}.parquet") \
    .withColumn("provider", lit("taxi"))

# Uber Reformat
uber_df = spark.read.parquet(f"data/cleaned/{YEAR}/uber/uber_tripdata_ultimate_{YEAR}.parquet") \
    .withColumn("provider", lit("uber")) \
    .withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
    .withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime") \
    .withColumnRenamed("trip_miles", "trip_distance") \
    .withColumnRenamed("base_passenger_fare", "fare_amount") \
    .withColumn("passenger_count", lit(None).cast("long")) \
    .withColumn("payment_type", lit(None).cast("string")) \

columns = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "trip_distance", "fare_amount", "payment_type",
    "PUBorough", "PUZone", "DOBorough", "DOZone", "provider"
] # Removed passenger_count

df_taxi_uber = taxi_df.select(columns).unionByName(uber_df.select(columns))

df_taxi_uber.printSchema()
df_taxi_uber.show(5, truncate=False)

df_taxi_uber.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Joined Taxi and Uber Data!")

spark.stop()
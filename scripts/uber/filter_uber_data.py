import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Increased Memory
spark = SparkSession.builder \
    .appName("FilterUberData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" # Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/uber/uber_tripdata_filtered_{YEAR}.parquet"

df = spark.read.parquet(f"data/raw/{YEAR}/uber/uber_tripdata_{YEAR}.parquet")

df = df.filter(
    col("hvfhs_license_num") == "HV0003"
)

df.printSchema()
df.show(5, truncate=False)

# Analysis Columns
joined_df = df.select(
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "base_passenger_fare",
    "PULocationID",
    "DOLocationID"
)

joined_df.printSchema()
joined_df.show(5, truncate=False)

joined_df.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Uber data filtered!")

spark.stop()
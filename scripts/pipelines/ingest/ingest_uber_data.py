import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("UberData") \
    .getOrCreate()

df = spark.read.parquet("data/raw/fhvhv_tripdata_2025-03.parquet")

df = df.filter(
    col("hvfhs_license_num") == "HV0003"
)

df.printSchema()

# Analysis Columns
df_sample = df.select(
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "base_passenger_fare",
    "PULocationID",
    "DOLocationID"
)

df_sample.show(5, truncate=False)

df_sample.write.parquet("data/cleaned/uber_analysis_sample.parquet", mode="overwrite")

spark.stop()
import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Increased Memory
spark = SparkSession.builder \
    .appName("CleanUberData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" #Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/uber/uber_tripdata_ultimate_{YEAR}.parquet"

df = spark.read.parquet(f"data/cleaned/{YEAR}/uber/uber_tripdata_uberzones_{YEAR}.parquet")

requiered_columns = [
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "base_passenger_fare",
    "PUBorough",
    "PUZone",
    "DOBorough",
    "DOZone"
]

df_clean = df.dropna(subset=requiered_columns)

df_clean = df.filter(
    (col("trip_miles") > 0) &
    (col("base_passenger_fare") > 0)
)

df_clean = df_clean.withColumn(
    "hvfhs_license_num",
    when(col("hvfhs_license_num") == "HV0003", "Uber")
)

df_clean.printSchema()
df_clean.show(10, truncate=False)

df_clean.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Uber Data Cleaned!")

spark.stop()
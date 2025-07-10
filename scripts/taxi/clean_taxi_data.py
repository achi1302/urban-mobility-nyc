import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Increased Memory
spark = SparkSession.builder \
    .appName("CleanTaxiData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" #Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_ultimate_{YEAR}.parquet"

df = spark.read.parquet(f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_taxizones_{YEAR}.parquet")

requiered_columns = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "fare_amount",
    "payment_type", "PUBorough", "PUZone", "DOBorough", "DOZone"
]

df_clean = df.dropna(subset=requiered_columns)

df_clean = df_clean.filter(
    (col("passenger_count") > 0) &
    (col("trip_distance") > 0) &
    (col("fare_amount") > 0) &
    (col("payment_type").isin([1, 2, 3, 4, 5, 6]))
)

df_clean = df_clean.withColumn(
    "payment_type",
    when(col("payment_type") == 1, "Credit Card")
    .when(col("payment_type") == 2, "Cash")
    .when(col("payment_type") == 3, "No Charge")
    .when(col("payment_type") == 4, "Dispute")
    .when(col("payment_type") == 5, "Unknown")
    .when(col("payment_type") == 6, "Voided Trip")
    .otherwise("Other")
)

df_clean.printSchema()
df_clean.show(10, truncate=False)

df_clean.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Taxi Data Cleaned!")

spark.stop()
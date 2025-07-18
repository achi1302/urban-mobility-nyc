import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

# Increased Memory
spark = SparkSession.builder \
    .appName("FilterTaxiData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" # Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_filtered_{YEAR}.parquet"

df = spark.read.parquet(f"data/raw/{YEAR}/taxi/yellowtaxi_tripdata_{YEAR}.parquet")

df.printSchema()
df.show(5, truncate=False)

# Analysis Columns
joined_df = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "payment_type",
    "PULocationID",
    "DOLocationID"
)

joined_df.printSchema()
joined_df.show(5, truncate=False)

joined_df.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Taxi data filtered!")

spark.stop()
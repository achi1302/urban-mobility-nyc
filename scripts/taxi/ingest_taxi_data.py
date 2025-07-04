import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()


from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("YellowTaxiTest") \
    .getOrCreate()

df = spark.read.parquet("data/raw/yellow_tripdata_2025-03.parquet")

df.printSchema()
df.show(5)

# Analysis Columns
df_sample = df.select(
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "payment_type",
    "PULocationID",
    "DOLocationID"
)

df_sample.show(5, truncate=False)

df_sample.write.parquet("data/cleaned/yellowtaxi_analysis_sample.parquet", mode="overwrite")

spark.stop()
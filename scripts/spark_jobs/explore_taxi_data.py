import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExploreYellowTaxi") \
    .getOrCreate()

df_cleaned = spark.read.parquet("data/cleaned/yellowtaxi_joined_zones.parquet")

print("\n Schema:")
df_cleaned.printSchema()

print("\n Sample Data:")
df_cleaned.show(10, truncate=False)

spark.stop()
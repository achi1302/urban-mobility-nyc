import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExploreYellowTaxi") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/yellowtaxi_analysis_sample.parquet")
df_cleaned = spark.read.parquet("data/cleaned/yellowtaxi_joined_zones.parquet")
df_final = spark.read.parquet("data/cleaned/yellowtaxi_final.parquet")

print("\n Schemas:")
df.printSchema()
df_cleaned.printSchema()
df_final.printSchema()

print("\n Sample Data:")
df.show(5, truncate=False)
df_cleaned.show(5, truncate=False)
df_final.show(5, truncate=False)

spark.stop()
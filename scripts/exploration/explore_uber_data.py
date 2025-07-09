import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExploreUber") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/uber/uber_analysis_sample.parquet")
df_joined_zones = spark.read.parquet("data/cleaned/uber/uber_joined_zones.parquet")
df_final = spark.read.parquet("data/cleaned/uber/uber_final.parquet")
df_yearly = spark.read.parquet("data/raw/2023/uber/uber_tripdata_2023.parquet")
df_yearly_filtered = spark.read.parquet("data/cleaned/2023/uber/uber_tripdata_filtered_2023")

print("\n NRows")
df_rows = df.count()
df_yearly_rows = df_yearly.count()
df_yearly_filtered_rows = df_yearly_filtered.count()
print(df_yearly_rows, df_yearly_filtered_rows, df_rows)

print("\n Schemas:")
df.printSchema()
df_joined_zones.printSchema()
df_final.printSchema()
df_yearly.printSchema()
df_yearly_filtered.printSchema()

print("\n Sample Data:")
df.show(5, truncate=False)
df_joined_zones.show(5, truncate=False)
df_final.show(5, truncate=False)
df_yearly.show(5, truncate=False)
df_yearly_filtered.show(5, truncate=False)


spark.stop()
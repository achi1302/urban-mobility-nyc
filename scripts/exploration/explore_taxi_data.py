import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExploreYellowTaxi") \
    .getOrCreate()

YEAR = "2023"

df = spark.read.parquet("data/cleaned/taxi/yellowtaxi_analysis_sample.parquet")


df_yearly = spark.read.parquet(f"data/raw/{YEAR}/taxi/yellowtaxi_tripdata_{YEAR}.parquet")
df_yearly_filtered = spark.read.parquet(f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_filtered_{YEAR}.parquet")
df_joined_zones = spark.read.parquet(f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_taxizones_{YEAR}.parquet")
df_final = spark.read.parquet(f"data/cleaned/{YEAR}/taxi/yellowtaxi_tripdata_ultimate_{YEAR}.parquet")
df_taxi_uber = spark.read.parquet(f"data/cleaned/{YEAR}/taxi_uber_tripdata_{YEAR}.parquet")

print("\n NRows:")
df_rows = df.count()


df_yearly_rows = df_yearly.count()
df_yearly_filtered_rows = df_yearly_filtered.count()
df_joined_zones_rows = df_joined_zones.count()
df_final_rows = df_final.count()
df_taxi_uber_rows = df_taxi_uber.count()
print(df_rows, df_yearly_rows, df_yearly_filtered_rows, df_joined_zones_rows, df_final_rows, df_taxi_uber_rows)

print("\n Schemas:")
df.printSchema()

df_yearly.printSchema()
df_yearly_filtered.printSchema()
df_joined_zones.printSchema()
df_final.printSchema()
df_taxi_uber.printSchema()

print("\n Sample Data:")
df.show(5, truncate=False)

df_yearly.show(5, truncate=False)
df_yearly_filtered.show(5, truncate=False)
df_joined_zones.show(5, truncate=False)
df_final.show(5, truncate=False)
df_taxi_uber.show(5, truncate=False)

spark.stop()
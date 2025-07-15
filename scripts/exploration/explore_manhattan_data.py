import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder \
    .appName("ExploreManhattanData") \
    .getOrCreate()

YEAR = "2023"

df = spark.read.parquet(f"data/cleaned/{YEAR}/taxi_uber_tripdata_{YEAR}.parquet")
df_manhattan = spark.read.parquet(f"data/cleaned/{YEAR}/manhattan_taxi_uber_tripdata_{YEAR}.parquet")
df_manhattan_zone_summary = spark.read.parquet(f"data/outputs/{YEAR}/manhattan_zone_summary_{YEAR}.parquet")
df_manhattan_provider_summary = spark.read.parquet(f"data/outputs/{YEAR}/manhattan_provider_summary_{YEAR}.parquet")

print("\n NRows")
df_rows = df.count()
df_manhattan_rows = df_manhattan.count()
print(df_rows, df_manhattan_rows)


print("\n Taxi Uber Data")
df.show(10, truncate=False)
df_manhattan.show(10, truncate=False)

print("\n Zone Summary:")
df_manhattan_zone_summary.orderBy("PURegion", "PUZone", "provider").filter(col("PUZone") == "Upper West Side North").show(10, truncate=False)

print("\n Provider Summary:")
df_manhattan_provider_summary.show(truncate=False)
import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExploreManhattanData") \
    .getOrCreate()

YEAR = "2023"

df = spark.read.parquet(f"data/cleaned/{YEAR}/taxi_uber_tripdata_{YEAR}.parquet")
df_manhattan = spark.read.parquet(f"data/cleaned/{YEAR}/manhattan_taxi_uber_tripdata_{YEAR}.parquet")

print("\n NRows")
df_rows = df.count()
df_manhattan_rows = df_manhattan.count()
print(df_rows, df_manhattan_rows)


print("\n Taxi Uber Data")
df.show(10, truncate=False)
df_manhattan.show(10, truncate=False)
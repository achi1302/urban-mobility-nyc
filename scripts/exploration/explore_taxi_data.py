import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ExploreYellowTaxi") \
    .getOrCreate()

df = spark.read.parquet("data/cleaned/taxi/yellowtaxi_analysis_sample.parquet")
df_joined_zones = spark.read.parquet("data/cleaned/taxi/yellowtaxi_joined_zones.parquet")
df_final = spark.read.parquet("data/cleaned/taxi/yellowtaxi_final.parquet")
df_yearly = spark.read.parquet("data/raw/2023/taxi/yearly/yellowtaxi_tripdata_2023.parquet")

print("\n NRows:")
df_rows = df.count()
df_joined_rows = df_joined_zones.count()
df_final_rows = df_final.count()
df_yearly_rows = df_yearly.count()
print(df_rows, df_joined_rows, df_final_rows, df_yearly_rows)

print("\n Schemas:")
df.printSchema()
df_joined_zones.printSchema()
df_final.printSchema()
df_yearly.printSchema()

print("\n Sample Data:")
df.show(5, truncate=False)
df_joined_zones.show(5, truncate=False)
df_final.show(5, truncate=False)
df_yearly.show(5, truncate=False)

#df_combined_rows = df_combined.count()
#print(df_combined_rows)
#df_combined.printSchema()
#df_combined.show(10, truncate=False)

spark.stop()
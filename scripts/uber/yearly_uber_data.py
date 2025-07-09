import findspark
import os
import shutil

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession


YEAR = "2023" # Change Year
MONTHS = [f"{i:02d}" for i in range(1, 13)]
TEMP_PATH = f"data/raw/{YEAR}/uber/temp"
OUTPUT_PATH = f"data/raw/{YEAR}/uber/uber_tripdata_{YEAR}.parquet"

# Increased Memory
spark = SparkSession.builder \
    .appName("UberYearlyData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Cleanup temp file
if os.path.exists(TEMP_PATH):
    shutil.rmtree(TEMP_PATH)

base_schema = None
first_file = True

for month in MONTHS:
    path = f"data/raw/{YEAR}/uber/tlc/fhvhv_tripdata_{YEAR}-{month}.parquet"
    if os.path.exists(path):
        print("Processing TLC data...")
        df = spark.read.parquet(path)

        if first_file:
            base_schema = df.schema
            df.write.parquet(TEMP_PATH, mode="overwrite")
            first_file = False
        else:
            # Align schema
            select_expr = [f"CAST(`{field.name}` AS {field.dataType.simpleString()}) AS `{field.name}`"
                           for field in base_schema]
            df = df.selectExpr(*select_expr)
            df.write.parquet(TEMP_PATH, mode="append")
    else:
        print("TLC File not found")

# Combine and repartition full year
print("Combining into yearly file...")
df_combined = spark.read.parquet(TEMP_PATH)
df_combined = df_combined.repartition(8) # Can't be to careful
df_combined.write.parquet(OUTPUT_PATH, mode="overwrite")

#Cleanup temp file
shutil.rmtree(TEMP_PATH)

print("Yearly dataset saved!")
spark.stop()
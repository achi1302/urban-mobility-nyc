import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession

# Increased Memory
spark = SparkSession.builder \
    .appName("CleanUberData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" # Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/manhattan_census_data_{YEAR}.parquet"

df = spark.read.csv(f"data/external/nhgis0004_ds266_{YEAR}_puma.csv", header=True, inferSchema=True)
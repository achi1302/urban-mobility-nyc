import findspark
import os 
from dotenv import load_dotenv

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

load_dotenv()

from pyspark.sql import SparkSession

jar_path = ",".join([
    "C:\\spark\\jars\\spark-snowflake_2.12-3.0.0.jar",
    "C:\\spark\\jars\\snowflake-jdbc-3.16.1.jar"
])


spark = SparkSession.builder \
        .appName("LoadToSnowflake") \
        .config("spark.jars", jar_path) \
        .config("spark.driver.extraClassPath", jar_path) \
        .getOrCreate()

YEAR = "2023" #Change Year

ZONE_SUMMARY = f"data/outputs/{YEAR}/manhattan_zone_summary_{YEAR}.parquet"
REGION_SUMMARY = f"data/outputs/{YEAR}/manhattan_region_summary_{YEAR}.parquet"
MONTHLY_REGION_TRENDS = f"data/outputs/{YEAR}/manhattan_monthly_region_trends_{YEAR}.parquet"
HOURLY_TRENDS = f"data/outputs/{YEAR}/manhattan_hourly_trends_{YEAR}.parquet"
PROVIDER_SUMMARY = f"data/outputs/{YEAR}/manhattan_provider_summary_{YEAR}.parquet"
PROVIDER_PREFERENCE_BY_WEALTH = f"data/outputs/{YEAR}/manhattan_provider_preference_by_wealth_{YEAR}.parquet"
PAYMENT_TYPE_SUMMARY = f"data/outputs/{YEAR}/manhattan_payment_type_{YEAR}.parquet"
FARE_PER_MILE_VS_PROPERTY_VALUE = f"data/outputs/{YEAR}/manhattan_fare_per_mile_vs_property_value_{YEAR}.parquet"
POP_DENSITY_VS_TRIP_VOLUME = f"data/outputs/{YEAR}/manhattan_population_density_vs_trip_volume_{YEAR}.parquet"
df = spark.read.parquet(POP_DENSITY_VS_TRIP_VOLUME)

sfOptions = {
    "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
    "sfUser": os.getenv("SNOWFLAKE_USER"),
    "sfPassword": os.getenv("SNOWFLAKE_PASSWORD"),
    "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    "sfDatabase": os.getenv("SNOWFLAKE_DATABASE"),
    "sfSchema": os.getenv("SNOWFLAKE_SCHEMA"),
    "sfRole": os.getenv("SNOWFLAKE_ROLE")
}

df.write \
        .format("snowflake") \
        .options(**sfOptions) \
        .option("dbtable", "MANHATTAN_POP_DENSITY_VS_TRIP_VOLUME") \
        .mode("overwrite") \
        .save()
#CHANGE TABLE NAME
print("Data loaded!")

spark.stop()
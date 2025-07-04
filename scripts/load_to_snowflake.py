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

df = spark.read.parquet("data/outputs/manhattan_mobility_by_region.parquet")

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
        .option("dbtable", "MANHATTAN_MOBILITY_BY_REGION") \
        .mode("overwrite") \
        .save()

print("Data loaded!")

spark.stop()
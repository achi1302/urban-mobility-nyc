import findspark
import os

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"

findspark.init()

from pyspark.sql import SparkSession

# Increased Memory
spark = SparkSession.builder \
    .appName("JoinTaxiZones") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" #Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/uber/uber_tripdata_uberzones_{YEAR}.parquet"

df = spark.read.parquet(f"data/cleaned/{YEAR}/uber/uber_tripdata_filtered_{YEAR}.parquet")

df_zones = spark.read.csv("data/external/taxi+_zone_lookup.csv", header=True)

# Pickup
df = df.join(
    df_zones.withColumnRenamed("LocationID", "PULocationID") \
        .withColumnRenamed("Borough", "PUBorough") \
        .withColumnRenamed("Zone", "PUZone"),
    on="PULocationID",
    how="left"
)

# Dropoff
df = df.join(
    df_zones.withColumnRenamed("LocationID", "DOLocationID") \
        .withColumnRenamed("Borough", "DOBorough") \
        .withColumnRenamed("Zone", "DOZone"),
    on="DOLocationID",
    how="left"
)

# Preview
joined_df = df.select(
    "hvfhs_license_num",
    "pickup_datetime",
    "dropoff_datetime",
    "trip_miles",
    "base_passenger_fare",
    "PUBorough",
    "PUZone",
    "DOBorough",
    "DOZone"
)

joined_df.printSchema()
joined_df.show(10, truncate=False)

#Save
joined_df.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Uber Zones Joined!")

spark.stop()
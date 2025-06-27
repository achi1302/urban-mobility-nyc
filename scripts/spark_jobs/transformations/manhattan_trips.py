import findspark
import os
import geopandas as gpd

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, udf
from pyspark.sql.types import StringType

spark = SparkSession.builder \
    .appName("ManhattanTrips") \
    .getOrCreate()

# STEP 1: Classify Zones
gpdf = gpd.read_file("data/external/taxi_zones/taxi_zones.shp")
gpdf["centroid_proj"] = gpdf.geometry.centroid


gpdf = gpdf.set_geometry("centroid_proj")
gpdf = gpdf.set_crs(gpdf.crs).to_crs(epsg=4326)
gpdf["lat"] = gpdf.geometry.y
gpdf["lon"] = gpdf.geometry.x

manhattan_gpdf = gpdf[gpdf["borough"] == "Manhattan"].copy()

def assign_region(lat):
    if lat < 40.736824: # Custom Margins
        return "Lower Manhattan"
    elif 40.736824 < lat < 40.7697680:
        return "Midtown Manhattan"
    elif  40.7697680 < lat < 40.876994:
        return "Upper Manhattan"
    return "Unknown"

manhattan_gpdf["region"] = manhattan_gpdf["lat"].apply(assign_region)

# Zone to Region Map
zone_region_map = manhattan_gpdf.set_index("zone")["region"].to_dict()
zone_region_map_bc = spark.sparkContext.broadcast(zone_region_map)

# PUZone to PURegion
@udf(StringType())
def map_zone_to_region(zone):
    return zone_region_map_bc.value.get(zone, "Unknown")

# STEP 2: Read and Transform
# Taxi
taxi_df = spark.read.parquet("data/cleaned/taxi/yellowtaxi_final.parquet") \
    .withColumn("provider", lit("taxi"))

# Uber Reformat
uber_df = spark.read.parquet("data/cleaned/uber/uber_final.parquet") \
    .withColumn("provider", lit("uber")) \
    .withColumnRenamed("pickup_datetime", "tpep_pickup_datetime") \
    .withColumnRenamed("dropoff_datetime", "tpep_dropoff_datetime") \
    .withColumnRenamed("trip_miles", "trip_distance") \
    .withColumnRenamed("base_passenger_fare", "fare_amount") \
    .withColumn("passenger_count", lit(None).cast("long")) \
    .withColumn("payment_type", lit(None).cast("string")) \

columns = [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "passenger_count",
    "trip_distance", "fare_amount", "payment_type",
    "PUBorough", "PUZone", "DOBorough", "DOZone", "provider"
]

df = taxi_df.select(columns).unionByName(uber_df.select(columns))

target_neighborhoods = list(zone_region_map.keys())
df = df.filter(
    (col("PUBorough") == "Manhattan") &
    (col("DOBorough") == "Manhattan") &
    (col("PUZone").isin(target_neighborhoods)) &
    (col("DOZone").isin(target_neighborhoods)) &
    (col("PUZone") != "Governor's Island/Ellis Island/Liberty Island") &
    (col("DOZone") != "Governor's Island/Ellis Island/Liberty Island")
)

df = df.withColumn("PURegion", map_zone_to_region(col("PUZone")))
df = df.withColumn("DORegion", map_zone_to_region(col("DOZone")))

df.select("PUZone", "PURegion", "DOZone", "DORegion", "provider", "fare_amount", "trip_distance").show(10, truncate=False)

df.write.parquet("data/cleaned/manhattan_trips.parquet", mode="overwrite")

spark.stop()



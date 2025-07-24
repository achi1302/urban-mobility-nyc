import findspark
import os
import geopandas as gpd

os.environ["JAVA_HOME"] = "C:\\java\\jdk-8"
os.environ["SPARK_HOME"] = "C:\\spark\\spark-3.5.5-bin-hadoop3"
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  udf, year
from pyspark.sql.types import StringType, DoubleType

# Increased Memory
spark = SparkSession.builder \
    .appName("ManhattanTaxiUberData") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

YEAR = "2023" #Change Year
OUTPUT_PATH = f"data/cleaned/{YEAR}/manhattan_taxi_uber_tripdata_{YEAR}.parquet"

# CLASSIFY ZONES
gpdf = gpd.read_file("data/external/taxi_regions/taxi_zones.shp")
gpdf["centroid_proj"] = gpdf.geometry.centroid

gpdf = gpdf.set_geometry("centroid_proj")
gpdf = gpdf.set_crs(gpdf.crs).to_crs(epsg=4326)
gpdf["lat"] = gpdf.geometry.y
gpdf["lon"] = gpdf.geometry.x

manhattan_gpdf = gpdf[gpdf["borough"] == "Manhattan"].copy()
def assign_region(lat):
    if lat < 40.736823: # Custom Margins
        return "Lower Manhattan"
    elif lat < 40.801169:
        return "Midtown Manhattan"
    elif lat < 40.876994:
        return "Upper Manhattan"
    return "Unknown"

manhattan_gpdf["region"] = manhattan_gpdf["lat"].apply(assign_region)

# Manual Overrides for Manhattan Valley and East Harlem South
mv_hs_override = {
    "Manhattan Valley": "Upper Manhattan",
    "East Harlem South": "Upper Manhattan"
}
manhattan_gpdf["region"] = manhattan_gpdf.apply(
    lambda row: mv_hs_override.get(row["zone"], row["region"]), axis=1
)
#print(manhattan_gpdf.sort_values(by="lat")[["zone", "lat", "lon", "region"]])

# Zone to Region Map
zone_region_map = manhattan_gpdf.set_index("zone")["region"].to_dict()
zone_region_map_bc = spark.sparkContext.broadcast(zone_region_map)

# Zone to Lat/Lon Maps
zone_lat_map = manhattan_gpdf.set_index("zone")["lat"].to_dict()
zone_lon_map = manhattan_gpdf.set_index("zone")["lon"].to_dict()
zone_lat_map_bc = spark.sparkContext.broadcast(zone_lat_map)
zone_lon_map_bc = spark.sparkContext.broadcast(zone_lon_map)

# PUZone to PURegion
@udf(StringType())
def map_zone_to_region(zone):
    return zone_region_map_bc.value.get(zone, "Unknown")

@udf(DoubleType())
def map_zone_to_lat(zone):
    return zone_lat_map_bc.value.get(zone, None)

@udf(DoubleType())
def map_zone_to_lon(zone):
    return zone_lon_map_bc.value.get(zone, None)

# READ AND TRANSFORM
df_taxi_uber = spark.read.parquet(f"data/cleaned/{YEAR}/taxi_uber_joined_tripdata_{YEAR}.parquet")
target_neighborhoods = list(zone_region_map.keys())

df_taxi_uber = df_taxi_uber.filter(
    (year("tpep_pickup_datetime") == 2023) &
    (year("tpep_dropoff_datetime") == 2023) &
    (col("PUBorough") == "Manhattan") &
    (col("DOBorough") == "Manhattan") &
    (col("PUZone").isin(target_neighborhoods)) &
    (col("DOZone").isin(target_neighborhoods)) &
    (col("PUZone") != "Governor's Island/Ellis Island/Liberty Island") &
    (col("DOZone") != "Governor's Island/Ellis Island/Liberty Island") &
    (col("PUZone") != "Randalls Island") &
    (col("DOZone") != "Randalls Island") & 
    (col("PUZone") != "Roosevelt Island") &
    (col("DOZone") != "Roosevelt Island")
)

df_taxi_uber = df_taxi_uber.withColumn("PURegion", map_zone_to_region(col("PUZone")))
df_taxi_uber = df_taxi_uber.withColumn("DORegion", map_zone_to_region(col("DOZone")))

df_taxi_uber = df_taxi_uber.withColumn("PULat", map_zone_to_lat(col("PUZone")))
df_taxi_uber = df_taxi_uber.withColumn("PULon", map_zone_to_lon(col("PUZone")))
df_taxi_uber = df_taxi_uber.withColumn("DOLat", map_zone_to_lat(col("PUZone")))
df_taxi_uber = df_taxi_uber.withColumn("DOLon", map_zone_to_lon(col("PUZone")))

print("\n Preview:")
df_taxi_uber.show(10, truncate=False)

df_taxi_uber.write.parquet(OUTPUT_PATH, mode="overwrite")

print("Manhattan Taxi vs Uber Data Created!")

spark.stop()


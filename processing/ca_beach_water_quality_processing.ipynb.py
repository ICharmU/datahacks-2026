# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "python-dotenv",
#   "xarray",
#   "netcdf4",
#   "fsspec",
#   "pyarrow",
#   "numpy<2",
# ]
# ///
import os
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------
# 1. Environment & Auth Setup
# ---------------------------------------------------------
load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('aws_secret_access_key')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('aws_region', 'us-west-2')

s3 = boto3.client('s3')
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def download_to_local(bucket, key):
    """Downloads a file from S3 to the local /tmp/ directory for Spark native reading."""
    file_name = key.split('/')[-1]
    local_path = f"/tmp/{file_name}"
    
    if not os.path.exists(local_path):
        print(f"Downloading {file_name}...")
        s3.download_file(bucket, key, local_path)
    else:
        print(f"Found {file_name} locally. Skipping download.")
        
    return f"file://{local_path}"

def apply_beach_tiling_pipeline(df, feature_cols):
    """
    Applies the standardized 500m tiling and feature scaling logic.
    Assumes Date, Lat_Dec, and Lon_Dec are already standardized.
    """
    # 1. Feature Scaling (Z-Score)
    stats = df.select([F.mean(c).alias(f"{c}_avg") for c in feature_cols] + 
                      [F.stddev(c).alias(f"{c}_std") for c in feature_cols]).collect()[0]
    
    scaled_names = []
    for c in feature_cols:
        avg, std = stats[f"{c}_avg"], stats[f"{c}_std"]
        col_name = f"{c}_scaled"
        # Standardize, guarding against division by zero
        df = df.withColumn(col_name, F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0)))
        scaled_names.append(col_name)

    # 2. Universal 500m Tiling
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("Lat_Dec") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("Lon_Dec") * (M_PER_DEG * F.cos(F.radians(F.col("Lat_Dec")))) / TILE_SIZE
        )
    )
    
    # 3. Final State Assembly
    df_final = df_tiled.withColumn(
        "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
    ).withColumn(
        "environmental_features", F.array(*scaled_names)
    )
    
    return df_final.select(
        "tile_id", 
        "conventional_date", 
        "Lon_Dec", 
        "Lat_Dec", 
        "environmental_features"
    )

# COMMAND ----------

run_date = "2026-04-19"
bucket_name = "toxictide-raw"

# Define keys based on your S3 structure
results_key = f"sources/ca_beach_water_quality/raw/run_date={run_date}/monitoring_results/beach_monitoring_results.csv"
stations_key = f"sources/ca_beach_water_quality/raw/run_date={run_date}/monitoring_stations/beach_monitoring_stations.csv"
closures_key = f"sources/ca_beach_water_quality/raw/run_date={run_date}/postings_closures/beach_postings_closures.csv"

# Download files to /tmp/ to avoid memory blowouts
results_path = download_to_local(bucket_name, results_key)
stations_path = download_to_local(bucket_name, stations_key)
closures_path = download_to_local(bucket_name, closures_key) # Available if you need closure events as labels

print("\nLoading datasets into Spark...")
# Load via Spark's native CSV reader
results_df = spark.read.option("header", "true").option("inferSchema", "true").csv(results_path)
stations_df = spark.read.option("header", "true").option("inferSchema", "true").csv(stations_path)

# --- Join Data & Standardize Schema ---
# The primary key connecting stations to results in CA Water Quality data is typically 'StationCode'
join_key = "StationCode" 

# Filter stations to only the spatial columns needed to keep the join performant
stations_subset = stations_df.select(
    F.col(join_key), 
    F.col("Latitude").alias("Lat_Dec"),   # Adjust if named differently in CSV
    F.col("Longitude").alias("Lon_Dec")   # Adjust if named differently in CSV
).dropDuplicates([join_key])

# Inner join results with spatial data
combined_df = results_df.join(stations_subset, on=join_key, how="inner")

# Standardize the date column
# Adjust 'SampleDate' and the date format ('MM/dd/yyyy' or 'yyyy-MM-dd') based on the CSV contents
combined_df = combined_df.withColumn(
    "conventional_date", 
    F.to_date(F.col("SampleDate"), "MM/dd/yyyy") 
)

# Define the target features to scale and array 
# CA Beach data often includes Enterococcus, Total Coliform, and Fecal Coliform.
TARGET_FEATURES = ["Enterococcus", "TotalColiform", "FecalColiform"]

# Drop rows missing crucial spatial/temporal anchors
clean_df = combined_df.dropna(subset=["Lat_Dec", "Lon_Dec", "conventional_date"])

print("\nApplying scaling and 500m tiling logic...")
final_output = apply_beach_tiling_pipeline(clean_df, TARGET_FEATURES)

display(final_output)

# Write to Delta
print("\nWriting to Delta table...")
final_output.write.format("delta").mode("overwrite").saveAsTable("default.ca_beach_wq_tiled")

# Optional: Clean up local /tmp/ files after successful write
os.remove(results_path.replace("file://", ""))
os.remove(stations_path.replace("file://", ""))
os.remove(closures_path.replace("file://", ""))

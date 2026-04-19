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
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession, functions as F

# ---------------------------------------------------------
# 1. Environment & Spark Optimization Setup
# ---------------------------------------------------------
load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('aws_secret_access_key')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('aws_region', 'us-west-2')

s3 = boto3.client('s3')
spark = SparkSession.builder.getOrCreate()

# Use the same logic for the list as we do for the columns
def slugify(name):
    import re
    name = re.sub(r'[^a-zA-Z0-9]', '_', name.lower().strip())
    return re.sub(r'_+', '_', name).strip('_')

# Define your raw names here
RAW_TARGETS = ["Enterococcus", "Total Coliform", "Fecal Coliform"]

# Automatically clean them so they match the DataFrame later
TARGET_FEATURES = [slugify(f) for f in RAW_TARGETS]

print(f"Targeting cleaned columns: {TARGET_FEATURES}")
RAW_TABLE = "default.ca_beach_wq_raw"
FINAL_TABLE = "default.ca_beach_wq_tiled"

# COMMAND ----------

import re

def clean_column_names(df):
    """
    Replaces all invalid characters in column names with underscores 
    to satisfy Delta Lake requirements.
    """
    # Replace any character that isn't a letter or number with '_'
    df.columns = [re.sub(r'[^a-zA-Z0-9]', '_', c.lower().strip()) for c in df.columns]
    # Collapse multiple underscores '___' into one '_'
    df.columns = [re.sub(r'_+', '_', c).strip('_') for c in df.columns]
    return df

# COMMAND ----------

run_date = "2026-04-19"
bucket_name = "toxictide-raw"

results_key = f"sources/ca_beach_water_quality/raw/run_date={run_date}/monitoring_results/beach_monitoring_results.csv"
stations_key = f"sources/ca_beach_water_quality/raw/run_date={run_date}/monitoring_stations/beach_monitoring_stations.csv"

print("Fetching station metadata...")
stations_obj = s3.get_object(Bucket=bucket_name, Key=stations_key)
stations_pdf = pd.read_csv(stations_obj['Body'])
stations_pdf = clean_column_names(stations_pdf)

# Now use the 'slugified' names
st_code_col = 'station_id'

# We calculate the centroid for Latitude and Longitude
if 'station_upperlat' in stations_pdf.columns and 'station_lowerlat' in stations_pdf.columns:
    stations_pdf['lat_dec'] = (stations_pdf['station_upperlat'] + stations_pdf['station_lowerlat']) / 2
    stations_pdf['lon_dec'] = (stations_pdf['station_upperlon'] + stations_pdf['station_lowerlon']) / 2
else:
    # Fallback to beach coordinates if station ones are missing
    stations_pdf['lat_dec'] = (stations_pdf['beach_upperlat'] + stations_pdf['beach_lowerlat']) / 2
    stations_pdf['lon_dec'] = (stations_pdf['beach_upperlon'] + stations_pdf['beach_lowerlon']) / 2

# Select and deduplicate
stations_subset = stations_pdf[[st_code_col, 'lat_dec', 'lon_dec']].copy()
stations_subset.columns = ['station_id', 'lat_dec', 'lon_dec']
stations_subset = stations_subset.drop_duplicates(subset=['station_id'])

print(f"Successfully mapped {len(stations_subset)} stations using centroids.")

print("Streaming massive results CSV in chunks...")
results_obj = s3.get_object(Bucket=bucket_name, Key=results_key)

# Read the stream in chunks to prevent OOM
chunk_size = 250000
csv_stream = pd.read_csv(results_obj['Body'], chunksize=chunk_size, low_memory=False)

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType

# 1. Define the parameters we are looking for in the 'parameter' column
# These must match the values inside the CSV's 'parameter' column exactly (case-sensitive)
RAW_PARAM_NAMES = ["Enterococcus", "Total Coliform", "Fecal Coliform"]
TARGET_FEATURES = [slugify(p) for p in RAW_PARAM_NAMES]

# 2. Update Schema to match pivoted results
base_schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("lat_dec", DoubleType(), True),
    StructField("lon_dec", DoubleType(), True),
    StructField("conventional_date", DateType(), True)
])
for feat in TARGET_FEATURES:
    base_schema.add(StructField(feat, DoubleType(), True))

# 3. Start Chunked Loop
print("Streaming results CSV in chunks, pivoting Long to Wide...")
results_obj = s3.get_object(Bucket=bucket_name, Key=results_key)
csv_stream = pd.read_csv(results_obj['Body'], chunksize=250000, low_memory=False)

is_first_chunk = True

print(f"Dropping existing table {RAW_TABLE} to ensure a clean schema...")
spark.sql(f"DROP TABLE IF EXISTS {RAW_TABLE}")
for i, chunk in enumerate(csv_stream):
    print(f"  -> Processing chunk {i + 1}...")
    chunk = clean_column_names(chunk)
    
    # --- STEP A: PIVOT THE DATA ---
    # Filter only for the bacteria parameters we care about
    # Note: 'parameter' and 'result' are the columns found in your printout
    chunk = chunk[chunk['parameter'].isin(RAW_PARAM_NAMES)]
    
    if chunk.empty:
        continue

    # Pivot: This turns 'parameter' values into column headers
    # We use 'result' as the values
    pivoted_chunk = chunk.pivot_table(
        index=['station_id', 'sampledate'], 
        columns='parameter', 
        values='result',
        aggfunc='first' # In case of duplicate samples on the same day
    ).reset_index()
    
    # Clean the new pivoted column names (e.g., 'Total Coliform' -> 'total_coliform')
    pivoted_chunk = clean_column_names(pivoted_chunk)
    
    # --- STEP B: JOIN AND CLEAN ---
    # Ensure station_id is string on both sides to prevent PySparkTypeError
    pivoted_chunk['station_id'] = pivoted_chunk['station_id'].astype(str)
    stations_subset['station_id'] = stations_subset['station_id'].astype(str)
    
    merged_pdf = pd.merge(pivoted_chunk, stations_subset, on='station_id', how='inner')
    merged_pdf['station_id'] = merged_pdf['station_id'].astype(str).replace('nan', None)
    
    # Flexible Date Parsing
    if 'sampledate' in merged_pdf.columns:
        merged_pdf['conventional_date'] = pd.to_datetime(merged_pdf['sampledate'], errors='coerce').dt.date

    # Drop rows missing crucial anchors
    merged_pdf = merged_pdf.dropna(subset=['lat_dec', 'lon_dec', 'conventional_date'])
    
    # Ensure all target columns exist (fill with NaN if a chunk is missing one)
    for feat in TARGET_FEATURES:
        if feat not in merged_pdf.columns:
            merged_pdf[feat] = None
        else:
            merged_pdf[feat] = pd.to_numeric(merged_pdf[feat], errors='coerce')

    if merged_pdf.empty:
        continue

    # --- STEP C: SELECT AND CONVERT ---
    final_cols = [field.name for field in base_schema]
    merged_pdf = merged_pdf[final_cols]
    
    # Final String Cast for station_id to satisfy Arrow/Spark
    merged_pdf['station_id'] = merged_pdf['station_id'].astype(str)

    chunk_sdf = spark.createDataFrame(merged_pdf, schema=base_schema)
    
    write_mode = "overwrite" if is_first_chunk else "append"
    if is_first_chunk:
        (chunk_sdf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true") # Force Delta to accept the new base_schema
        .saveAsTable(RAW_TABLE))
    else:
        (chunk_sdf.write
        .format("delta")
        .mode("append")
        .saveAsTable(RAW_TABLE))
    
    is_first_chunk = False
    del chunk, pivoted_chunk, merged_pdf, chunk_sdf

# COMMAND ----------

print(f"\nIngestion complete. Loading full dataset natively from {RAW_TABLE}...")

# Let Spark read the Delta table (Highly optimized, immune to Pandas OOM limits)
full_df = spark.read.table(RAW_TABLE)

# Calculate GLOBAL statistics for accurate Z-score scaling
print("Calculating global environmental statistics...")
stats = full_df.select([F.mean(c).alias(f"{c}_avg") for c in TARGET_FEATURES] + 
                       [F.stddev(c).alias(f"{c}_std") for c in TARGET_FEATURES]).collect()[0]

scaled_names = []
for c in TARGET_FEATURES:
    avg, std = stats[f"{c}_avg"], stats[f"{c}_std"]
    col_name = f"{c}_scaled"
    full_df = full_df.withColumn(col_name, F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0)))
    scaled_names.append(col_name)

print("Applying 500m tiling logic...")
TILE_SIZE = 500 
M_PER_DEG = 111320

df_tiled = full_df.withColumn(
    "lat_idx", F.floor(F.col("lat_dec") * M_PER_DEG / TILE_SIZE)
).withColumn(
    "lon_idx", F.floor(
        F.col("lon_dec") * (M_PER_DEG * F.cos(F.radians(F.col("lat_dec")))) / TILE_SIZE
    )
)

# COMMAND ----------

df_final = df_tiled.withColumn(
    "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
).withColumn(
    "environmental_features", F.array(*scaled_names)
)

final_output = df_final.select(
    "tile_id", 
    "conventional_date", 
    "lon_dec", 
    "lat_dec", 
    "environmental_features"
)

print(f"Writing final scaled tiles to {FINAL_TABLE}...")
final_output.write.format("delta").mode("overwrite").saveAsTable(FINAL_TABLE)

print("Pipeline Complete!")

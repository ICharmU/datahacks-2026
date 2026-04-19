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
from functools import reduce
from pyspark.sql import SparkSession, DataFrame, functions as F

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

STATION_COORDS = {
    "9410170": {"lat": 32.714, "lon": -117.236} # San Diego (Quarantine Station)
}

# ---------------------------------------------------------
# 3. Core Extraction Logic 
# ---------------------------------------------------------
def process_noaa_tides_csv(s3_path):
    """
    Downloads a NOAA Tide CSV, applies hardcoded coordinates from the dictionary,
    and dynamically extracts all numeric environmental features.
    """
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_filename = f"/tmp/{key.split('/')[-1]}"
    
    # Extract Station ID from S3 Key 
    try:
        station_id = [p for p in path_parts if p.startswith('station_')][0].split('_')[1]
    except IndexError:
        print(f"  -> [ERROR] Could not parse station ID from path: {s3_path}")
        return None, []
        
    coords = STATION_COORDS.get(station_id)
    if not coords:
        print(f"  -> [WARNING] Missing coordinate mapping for Station ID {station_id}. Skipping.")
        return None, []
        
    print(f"  -> Downloading {local_filename}...")
    s3.download_file(bucket_name, key, local_filename)
    
    try:
        pdf = pd.read_csv(local_filename)
        
        # Standardize columns to lowercase and replace spaces with underscores
        pdf.columns = [c.lower().strip().replace(' ', '_') for c in pdf.columns]
        
        # ---> FIX: Drop any duplicate columns that might have been created <---
        pdf = pdf.loc[:, ~pdf.columns.duplicated()]
        
        # Bulletproof Date Parsing
        time_col = next((c for c in pdf.columns if 'date' in c or 'time' in c), None)
        if time_col:
            pdf['conv_date_str'] = pd.to_datetime(pdf[time_col]).dt.strftime('%m/%d/%Y')
        else:
            print(f"  -> [ERROR] No recognizable date column found. Columns are: {pdf.columns}")
            return None, []
            
        # Inject Hardcoded Spatial Anchors
        pdf['Lat_Dec'] = float(coords['lat'])
        pdf['Lon_Dec'] = float(coords['lon'])
        pdf['station_id'] = str(station_id)
        
        # Dynamically define target features
        numeric_cols = pdf.select_dtypes(include=['number']).columns.tolist()
        vars_to_keep = [c for c in numeric_cols if c not in ['lat_dec', 'lon_dec']]
        
        if not vars_to_keep:
            print(f"  -> [ERROR] No numeric features found in file. Columns are: {pdf.columns}")
            return None, []
            
        keep_cols = ['station_id', 'Lat_Dec', 'Lon_Dec', 'conv_date_str'] + vars_to_keep
        
        # Drop rows where ALL environmental features are missing
        clean_pdf = pdf[keep_cols].dropna(subset=vars_to_keep, how='all')
        
        # ---> FIX: Safely iterate using .dtypes.items() to prevent DataFrame dtype errors <---
        for col, col_dtype in clean_pdf.dtypes.items():
            if col_dtype == 'object':
                clean_pdf[col] = clean_pdf[col].astype(str)
            elif 'float' in str(col_dtype):
                clean_pdf[col] = clean_pdf[col].astype(float)
                
        sdf = spark.createDataFrame(clean_pdf)
        return sdf, vars_to_keep
            
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)

# COMMAND ----------

def apply_tides_pipeline(df, feature_cols):
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
    # Scale all features dynamically
    stats = df.select([F.mean(c).alias(f"{c}_avg") for c in feature_cols] + 
                      [F.stddev(c).alias(f"{c}_std") for c in feature_cols]).collect()[0]
    
    scaled_names = []
    for c in feature_cols:
        avg, std = stats[f"{c}_avg"], stats[f"{c}_std"]
        col_name = f"{c}_scaled"
        df = df.withColumn(col_name, F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0)))
        scaled_names.append(col_name)

    # 500m Tiling Logic (Reactivated)
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("Lat_Dec") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("Lon_Dec") * (M_PER_DEG * F.cos(F.radians(F.col("Lat_Dec")))) / TILE_SIZE
        )
    )
    
    df_final = df_tiled.withColumn(
        "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
    ).withColumn(
        "environmental_features", F.array(*scaled_names)
    )
    
    return df_final.select(
        "station_id",
        "tile_id", 
        "conventional_date", 
        "Lon_Dec", 
        "Lat_Dec", 
        "environmental_features"
    )

# COMMAND ----------

run_date = "2026-04-19"
bucket_name = "toxictide-raw"
tides_prefix = f"sources/noaa_tides/raw/run_date={run_date}/"

print(f"Scanning for NOAA Tide CSVs under: s3://{bucket_name}/{tides_prefix}")

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=tides_prefix)

all_spark_dfs = []

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            
            if key.endswith('.csv'):
                full_s3_path = f"s3://{bucket_name}/{key}"
                print(f"\nProcessing Tide File: {full_s3_path}")
                
                raw_sdf, found_features = process_noaa_tides_csv(full_s3_path)
                
                if raw_sdf is None:
                    continue
                    
                processed_df = apply_tides_pipeline(raw_sdf, found_features)
                all_spark_dfs.append(processed_df)

# ---------------------------------------------------------
# 6. Union and Save
# ---------------------------------------------------------
if all_spark_dfs:
    print(f"\nSuccessfully processed {len(all_spark_dfs)} tide files. Unioning datasets...")
    combined_tides_df = reduce(DataFrame.unionByName, all_spark_dfs)
    
    print("Writing combined dataset to Delta table...")
    combined_tides_df.write.format("delta").mode("overwrite").saveAsTable("default.noaa_tides_tiled_all")
    print("Done!")
else:
    print("No valid NOAA Tide data was processed.")

# COMMAND ----------

if all_spark_dfs:
    print(f"\nSuccessfully processed {len(all_spark_dfs)} tide files. Unioning datasets...")
    combined_tides_df = reduce(DataFrame.unionByName, all_spark_dfs)
    
    print("Writing combined dataset to Delta table...")
    combined_tides_df.write.format("delta").mode("overwrite").saveAsTable("default.noaa_tides_tiled_all")
    print("Done!")
else:
    print("No valid NOAA Tide data was processed.")

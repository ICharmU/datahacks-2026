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
import xarray as xr
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

def process_cdip_nc_local(s3_path, target_features):
    """
    Downloads a CDIP NetCDF file, extracts station coordinates and target 
    wave/sst features, flattens to Pandas, and converts to Spark.
    """
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_filename = f"/tmp/{key.split('/')[-1]}"
    
    print(f"  -> Downloading {local_filename}...")
    s3.download_file(bucket_name, key, local_filename)
    
    try:
        with xr.open_dataset(local_filename, engine='netcdf4') as ds:
            # CDIP Standard naming fallbacks for Coordinates
            def get_cdip_coord(ds, standard_name, cdip_meta_name):
                if cdip_meta_name in ds:
                    return float(ds[cdip_meta_name].values[0])
                elif standard_name in ds:
                    return float(ds[standard_name].values[0])
                return 0.0

            lat_val = get_cdip_coord(ds, 'latitude', 'metaStationLatitude')
            lon_val = get_cdip_coord(ds, 'longitude', 'metaStationLongitude')
            
            # Standardize variable casing to lowercase
            ds = ds.rename({v: v.lower() for v in ds.variables})
            
            # Filter dataset to target variables to save memory
            vars_to_keep = [v for v in target_features if v in ds.data_vars]
            if not vars_to_keep:
                return None, []
                
            # Flatten to Pandas
            pdf = ds[vars_to_keep].to_dataframe().reset_index()
            
            # Standardize Coordinates
            pdf['Lat_Dec'] = lat_val
            pdf['Lon_Dec'] = lon_val
            
            # Standardize Date (CDIP usually uses 'wavetime', 'ssttime', or 'time')
            time_cols = [c for c in pdf.columns if 'time' in c.lower()]
            if time_cols:
                # Use the first time column found (e.g., wavetime)
                pdf['conv_date_str'] = pd.to_datetime(pdf[time_cols[0]]).dt.strftime('%m/%d/%Y')
            else:
                return None, [] # Skip if we can't anchor the data in time
                
            # Keep only necessary columns
            keep_cols = ['Lat_Dec', 'Lon_Dec', 'conv_date_str'] + vars_to_keep
            existing_cols = [c for c in keep_cols if c in pdf.columns]
            
            clean_pdf = pdf[existing_cols].dropna(subset=['Lat_Dec', 'Lon_Dec'])
            
            # Spark Compatibility Cleanup
            for col in clean_pdf.columns:
                if clean_pdf[col].dtype == 'object':
                    clean_pdf[col] = clean_pdf[col].astype(str)
                elif 'float' in str(clean_pdf[col].dtype):
                    clean_pdf[col] = clean_pdf[col].astype(float)
                    
            sdf = spark.createDataFrame(clean_pdf)
            return sdf, vars_to_keep
            
    finally:
        # Prevent /tmp/ storage exhaustion
        if os.path.exists(local_filename):
            os.remove(local_filename)

# COMMAND ----------

def apply_cdip_tiling_pipeline(df, feature_cols):
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
    # Scale features
    stats = df.select([F.mean(c).alias(f"{c}_avg") for c in feature_cols] + 
                      [F.stddev(c).alias(f"{c}_std") for c in feature_cols]).collect()[0]
    
    scaled_names = []
    for c in feature_cols:
        avg, std = stats[f"{c}_avg"], stats[f"{c}_std"]
        col_name = f"{c}_scaled"
        df = df.withColumn(col_name, F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0)))
        scaled_names.append(col_name)

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
        "tile_id", 
        "conventional_date", 
        "Lon_Dec", 
        "Lat_Dec", 
        "environmental_features"
    )

# COMMAND ----------

# ---------------------------------------------------------
# 4. Batch Execution Logic
# ---------------------------------------------------------
run_date = "2026-04-19"
bucket_name = "toxictide-raw"

# Root prefix catches all station subfolders automatically
cdip_prefix = f"sources/cdip/raw/run_date={run_date}/"

# Common CDIP variables (lowercase): 
# wavehs (Significant Wave Height), wavetp (Peak Wave Period), sst (Sea Surface Temp)
TARGET_FEATURES = ['wavehs', 'wavetp', 'sst']

print(f"Scanning for CDIP NetCDF files under: s3://{bucket_name}/{cdip_prefix}")

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=cdip_prefix)

all_spark_dfs = []

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            
            if key.endswith('.nc'):
                full_s3_path = f"s3://{bucket_name}/{key}"
                print(f"\nProcessing CDIP Station File: {full_s3_path}")
                
                try:
                    # 1. Extract & Convert
                    raw_sdf, found_features = process_cdip_nc_local(full_s3_path, TARGET_FEATURES)
                    
                    if raw_sdf is None:
                        print(f"  -> Skipping (no valid target data or time mapping found)")
                        continue
                        
                    # 2. Transform & Tile
                    processed_df = apply_cdip_tiling_pipeline(raw_sdf, found_features)
                    all_spark_dfs.append(processed_df)
                    
                except Exception as e:
                    print(f"  -> FAILED processing {key}: {e}")

# COMMAND ----------

if all_spark_dfs:
    print(f"\nSuccessfully processed {len(all_spark_dfs)} station files. Unioning datasets...")
    combined_cdip_df = reduce(DataFrame.unionByName, all_spark_dfs)
    
    print("Writing combined dataset to Delta table...")
    combined_cdip_df.write.format("delta").mode("overwrite").saveAsTable("default.cdip_tiled_all")
    print("Done!")
else:
    print("No valid CDIP data was processed.")

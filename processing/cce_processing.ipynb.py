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
from pyspark.sql import SparkSession, functions as F

load_dotenv()

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('aws_secret_access_key')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('aws_region', 'us-west-2')

# Test the connection with boto3
s3 = boto3.client('s3')
try:
    s3.list_objects_v2(Bucket="toxictide-raw", MaxKeys=1)
    print("Credentials successfully located and verified.")
except Exception as e:
    print(f"S3 connection failed: {e}")

# Anchor the Spark Session
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

def process_noaa_nc_local(s3_path):
    # Path and Download logic
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_filename = f"/tmp/{key.split('/')[-1]}"
    
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, key, local_filename)
    
    try:
        # Open Dataset
        with xr.open_dataset(local_filename, engine='netcdf4') as ds:
            
            # Helper to safely extract a single float from attributes or arrays
            def get_coord(ds, name):
                val = ds.attrs.get(name, ds.get(name))
                if val is None: return 0.0
                try:
                    return float(val.values.flatten()[0]) if hasattr(val, 'values') else float(val)
                except:
                    return 0.0

            lat_val = get_coord(ds, 'LATITUDE')
            lon_val = get_coord(ds, 'LONGITUDE')
            
            # Standardize casing to lowercase
            ds = ds.rename({v: v.lower() for v in ds.variables})
            
            # Flatten to Pandas
            pdf = ds.to_dataframe().reset_index()
            
            # Coordinate and Date Standardization
            pdf['Lat_Dec'] = lat_val
            pdf['Lon_Dec'] = lon_val
            
            if 'time' in pdf.columns:
                pdf['conv_date_str'] = pd.to_datetime(pdf['time']).dt.strftime('%m/%d/%Y')
            
            # Spark Compatibility Cleanup
            for col in pdf.columns:
                if pdf[col].dtype == 'object':
                    pdf[col] = pdf[col].astype(str)
                elif 'float' in str(pdf[col].dtype):
                    pdf[col] = pdf[col].astype(float)
            
            sdf = spark.createDataFrame(pdf)
            
            # Return lowercase list of features (data variables only)
            return sdf, [v.lower() for v in ds.data_vars]
            
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)

# COMMAND ----------

def apply_noaa_tiling_pipeline(df, feature_cols):
    # Date Conversion
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
    # Manual Scaling Loop
    stats = df.select([F.mean(c).alias(f"{c}_avg") for c in feature_cols] + 
                      [F.stddev(c).alias(f"{c}_std") for c in feature_cols]).collect()[0]
    
    scaled_names = []
    for c in feature_cols:
        avg, std = stats[f"{c}_avg"], stats[f"{c}_std"]
        col_name = f"{c}_scaled"
        df = df.withColumn(col_name, F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0)))
        scaled_names.append(col_name)

    # Universal 500m Tiling
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("Lat_Dec") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("Lon_Dec") * (M_PER_DEG * F.cos(F.radians(F.col("Lat_Dec")))) / TILE_SIZE
        )
    )
    
    # Final State Formatting
    df_final = df_tiled.withColumn(
        "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
    ).withColumn(
        "environmental_features", F.array(*scaled_names)
    )
    
    return df_final.select(
        "conventional_date", 
        "Lon_Dec", 
        "Lat_Dec", 
        "tile_id", 
        "environmental_features"
    )

# COMMAND ----------

run_date = "2026-04-19"
bucket_name = "toxictide-raw"

# By stopping at the run_date, this prefix will naturally catch both cce1/ and cce2/ folders
cce_root_prefix = f"sources/cce_moorings/raw/run_date={run_date}/"

print(f"Scanning for NetCDF files under: s3://{bucket_name}/{cce_root_prefix}")

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=cce_root_prefix)

all_cce_dataframes = []

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            
            # Only process NetCDF files
            if key.endswith('.nc'):
                full_s3_path = f"s3://{bucket_name}/{key}"
                print(f"Processing: {full_s3_path}")
                
                try:
                    # 1. Extract
                    raw_sdf, feature_vars = process_noaa_nc_local(full_s3_path)
                    
                    # 2. Filter features
                    relevant_features = [v for v in feature_vars if v in ['temp', 'psal', 'depth', 'cndc']]
                    
                    if not relevant_features:
                        print(f"  -> Skipping (no target features found)")
                        continue
                    
                    # 3. Transform
                    processed_df = apply_noaa_tiling_pipeline(raw_sdf, relevant_features)
                    
                    # Append to our list of DataFrames
                    all_cce_dataframes.append(processed_df)
                    
                except Exception as e:
                    print(f"  -> FAILED processing {key}: {e}")

# ---------------------------------------------------------
# 4. Union and Save
# ---------------------------------------------------------
if all_cce_dataframes:
    print(f"\nSuccessfully processed {len(all_cce_dataframes)} files. Unioning datasets...")
    
    # Merge all the individual Spark DataFrames into one
    combined_cce_df = reduce(DataFrame.unionByName, all_cce_dataframes)
    
    print("Writing combined dataset to Delta table...")
    combined_cce_df.write.format("delta").mode("overwrite").saveAsTable("default.cce_tiled_all")
    print("Done!")
else:
    print("No valid NetCDF files with the target features were found.")

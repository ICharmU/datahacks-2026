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
import json
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

def process_nws_marine_json(s3_path):
    """
    Downloads an NWS JSON file, flattens the nested structures, dynamically 
    extracts coordinates and time, and keeps all numeric features.
    """
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_filename = f"/tmp/{key.split('/')[-1]}"
    
    print(f"  -> Downloading {local_filename}...")
    s3.download_file(bucket_name, key, local_filename)
    
    try:
        with open(local_filename, 'r') as f:
            raw_json = json.load(f)
            
        # Flatten the JSON. 
        # If it's a list of records, it flattens each. If it's one large dictionary, it makes a 1-row DataFrame.
        pdf = pd.json_normalize(raw_json)
        
        # Standardize columns: lowercase, replace spaces and dots (from flattening) with underscores
        pdf.columns = [c.lower().strip().replace(' ', '_').replace('.', '_') for c in pdf.columns]
        
        # Drop duplicated columns safely
        pdf = pdf.loc[:, ~pdf.columns.duplicated()]
        
        # 1. Hunt for Date/Time
        # NWS data often uses 'validtime', 'timestamp', 'time', or nested variations
        time_col = next((c for c in pdf.columns if 'date' in c or 'time' in c), None)
        if time_col:
            # Handle NWS ISO 8601 strings (e.g., "2026-04-19T00:00:00+00:00/PT1H")
            # We split on '/' in case it's a duration string, then parse the datetime
            raw_time_series = pdf[time_col].astype(str).str.split('/').str[0]
            pdf['conv_date_str'] = pd.to_datetime(raw_time_series, errors='coerce').dt.strftime('%m/%d/%Y')
        else:
            print(f"  -> [ERROR] No recognizable date/time column found. Columns are: {pdf.columns}")
            return None, []
            
        # 2. Hunt for Spatial Coordinates
        # NWS typically stores these as geometry_coordinates or properties_latitude
        lat_col = next((c for c in pdf.columns if 'lat' in c), None)
        lon_col = next((c for c in pdf.columns if 'lon' in c), None)
        
        # Sometimes NWS puts coordinates in a single array: geometry_coordinates = [-117.1, 32.5]
        coord_array_col = next((c for c in pdf.columns if 'coordinates' in c), None)
        
        if lat_col and lon_col:
            pdf['Lat_Dec'] = pdf[lat_col].astype(float)
            pdf['Lon_Dec'] = pdf[lon_col].astype(float)
        elif coord_array_col and isinstance(pdf[coord_array_col].iloc[0], list):
            # GeoJSON standard is [Longitude, Latitude]
            pdf['Lon_Dec'] = pdf[coord_array_col].apply(lambda x: float(x[0]) if isinstance(x, list) and len(x) >= 2 else None)
            pdf['Lat_Dec'] = pdf[coord_array_col].apply(lambda x: float(x[1]) if isinstance(x, list) and len(x) >= 2 else None)
        else:
            print(f"  -> [ERROR] Could not parse spatial coordinates. Columns are: {pdf.columns}")
            return None, []
            
        # 3. Dynamically define target features
        # Keep only numeric columns, ignoring our standard routing columns
        numeric_cols = pdf.select_dtypes(include=['number']).columns.tolist()
        vars_to_keep = [c for c in numeric_cols if c.lower() not in ['lat_dec', 'lon_dec', lat_col, lon_col]]
        
        if not vars_to_keep:
            print(f"  -> [ERROR] No numeric features found in file. Columns are: {pdf.columns}")
            return None, []
            
        keep_cols = ['Lat_Dec', 'Lon_Dec', 'conv_date_str'] + vars_to_keep
        
        # Drop rows missing crucial anchors
        clean_pdf = pdf[keep_cols].dropna(subset=['Lat_Dec', 'Lon_Dec', 'conv_date_str'])
        clean_pdf = clean_pdf.dropna(subset=vars_to_keep, how='all') # Ensure at least one feature has data
        
        if clean_pdf.empty:
            return None, []
        
        # 4. Spark Compatibility Cleanup
        for col, col_dtype in clean_pdf.dtypes.items():
            if col_dtype == 'object':
                clean_pdf[col] = clean_pdf[col].astype(str)
            elif 'float' in str(col_dtype):
                clean_pdf[col] = clean_pdf[col].astype(float)
                
        sdf = spark.createDataFrame(clean_pdf)
        return sdf, vars_to_keep
            
    except Exception as e:
        print(f"  -> [ERROR] Failed parsing JSON {local_filename}: {e}")
        return None, []
        
    finally:
        if os.path.exists(local_filename):
            os.remove(local_filename)

# COMMAND ----------

def apply_nws_pipeline(df, feature_cols):
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

run_date = "2026-04-19"
bucket_name = "toxictide-raw"
nws_prefix = f"sources/nws_marine/raw/run_date={run_date}/"

print(f"Scanning for NWS JSON files under: s3://{bucket_name}/{nws_prefix}")

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=nws_prefix)

all_spark_dfs = []

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            
            if key.endswith('.json'):
                full_s3_path = f"s3://{bucket_name}/{key}"
                print(f"\nProcessing JSON File: {full_s3_path}")
                
                raw_sdf, found_features = process_nws_marine_json(full_s3_path)
                
                if raw_sdf is None:
                    continue
                    
                processed_df = apply_nws_pipeline(raw_sdf, found_features)
                all_spark_dfs.append(processed_df)

# ---------------------------------------------------------
# 5. Union and Save
# ---------------------------------------------------------
if all_spark_dfs:
    print(f"\nSuccessfully processed {len(all_spark_dfs)} JSON files. Unioning datasets...")
    
    # Use unionByName with allowMissingColumns=True because different locations 
    # might have different API metadata properties returned.
    combined_nws_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_spark_dfs)
    
    print("Writing combined dataset to Delta table...")
    combined_nws_df.write.format("delta").mode("overwrite").saveAsTable("default.nws_marine_tiled_all")
    print("Done!")
else:
    print("No valid NWS Marine data was processed.")

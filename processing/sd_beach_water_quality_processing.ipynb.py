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

def process_sd_beach_csv(s3_path):
    """
    Downloads the SD Beach Water Quality CSV, cleans detection limit operators 
    (like < and >), safely loops columns to cast numerics, and extracts features.
    """
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_filename = f"/tmp/{key.split('/')[-1]}"
    
    print(f"  -> Downloading {local_filename}...")
    s3.download_file(bucket_name, key, local_filename)
    
    try:
        pdf = pd.read_csv(local_filename)
        
        # Standardize columns to lowercase and replace spaces with underscores
        pdf.columns = [c.lower().strip().replace(' ', '_') for c in pdf.columns]
        
        # Drop any duplicate columns
        pdf = pdf.loc[:, ~pdf.columns.duplicated()]
        
        # WATER QUALITY FIX: Strip out '<', '>', 'E' (estimated), and 'ND' (Not Detected)
        pdf = pdf.replace(to_replace=[r'^<', r'^>', r'^E', r'^ND'], value='', regex=True)
        
        # Safely cast features to numeric, skipping our known string columns
        for col in pdf.columns:
            if any(skip_word in col for skip_word in ['date', 'time', 'sample', 'station', 'location', 'beach']):
                continue
            
            # Using 'coerce' turns any remaining un-parseable text into NaN, 
            # ensuring the column successfully converts to float64 for scaling.
            pdf[col] = pd.to_numeric(pdf[col], errors='coerce')
        
        # 1. Hunt for Date/Time
        time_col = next((c for c in pdf.columns if 'date' in c or 'time' in c or 'sample' in c), None)
        if time_col:
            pdf['conv_date_str'] = pd.to_datetime(pdf[time_col], errors='coerce').dt.strftime('%m/%d/%Y')
        else:
            print(f"  -> [ERROR] No recognizable date column found. Columns are: {pdf.columns}")
            return None, []
            
        # 2. Hunt for Spatial Coordinates
        lat_col = next((c for c in pdf.columns if 'lat' in c), None)
        lon_col = next((c for c in pdf.columns if 'lon' in c or 'lng' in c), None)
        
        if lat_col and lon_col:
            pdf['Lat_Dec'] = pdf[lat_col].astype(float)
            pdf['Lon_Dec'] = pdf[lon_col].astype(float)
        else:
            # Fallback if coordinates are missing
            station_col = next((c for c in pdf.columns if 'station' in c or 'location' in c or 'beach' in c), None)
            station_col = 'date'
            if station_col:
                print(f"  -> [WARNING] No Lat/Lon found. Passing through '{station_col}' as primary key.")
                pdf['Lat_Dec'] = None
                pdf['Lon_Dec'] = None
                pdf['station_id'] = pdf[station_col].astype(str)
            else:
                print(f"  -> [ERROR] No spatial coordinates or station IDs found.")
                return None, []
            
        # 3. Dynamically define target features (Only keep numeric columns for scaling)
        numeric_cols = pdf.select_dtypes(include=['number']).columns.tolist()
        
        # Exclude known non-environmental numbers (like IDs, ZIP codes, and coordinates)
        exclude_list = ['lat_dec', 'lon_dec', 'zip', 'id', 'year', 'month', 'day']
        if lat_col: exclude_list.append(lat_col)
        if lon_col: exclude_list.append(lon_col)
        
        pdf['conventional_date'] = pd.to_datetime(pdf['date'], errors='coerce').dt.date
        pdf = pdf.dropna(subset=['conventional_date'])
        vars_to_keep = [c for c in numeric_cols if not any(ex in c for ex in exclude_list)]
        
        if not vars_to_keep:
            print(f"  -> [ERROR] No numeric features found in file. Columns are: {pdf.columns}")
            return None, []
            
        # 4. Filter and Clean
        keep_cols = ['conv_date_str', 'Lat_Dec', 'Lon_Dec'] + vars_to_keep
        if 'station_id' in pdf.columns:
            keep_cols.insert(0, 'station_id')
            
        # Drop rows where ALL environmental features are missing
        clean_pdf = pdf[keep_cols].dropna(subset=vars_to_keep, how='all')
        
        # Spark Compatibility Cleanup
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

def apply_sd_wq_pipeline(df, feature_cols):
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

    # 500m Tiling Logic (With Null Handling for missing coordinates)
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    df_tiled = df.withColumn(
        "lat_idx", 
        F.when(F.col("Lat_Dec").isNotNull(), F.floor(F.col("Lat_Dec") * M_PER_DEG / TILE_SIZE)).otherwise(F.lit(None))
    ).withColumn(
        "lon_idx", 
        F.when(F.col("Lon_Dec").isNotNull(), F.floor(
            F.col("Lon_Dec") * (M_PER_DEG * F.cos(F.radians(F.col("Lat_Dec")))) / TILE_SIZE
        )).otherwise(F.lit(None))
    )
    
    df_final = df_tiled.withColumn(
        "tile_id", 
        F.when(F.col("lat_idx").isNotNull() & F.col("lon_idx").isNotNull(), 
               F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))).otherwise(F.lit(None))
    ).withColumn(
        "environmental_features", F.array(*scaled_names)
    )
    
    # Select columns dynamically to pass 'station_id' if we had to create it
    final_cols = ["tile_id", "conventional_date", "Lon_Dec", "Lat_Dec", "environmental_features"]
    if "station_id" in df.columns:
        final_cols.insert(0, "station_id")
        
    return df_final.select(*final_cols)

# COMMAND ----------

run_date = "2026-04-19"
bucket_name = "toxictide-raw"
sd_wq_prefix = f"sources/sd_beach_water_quality/raw/run_date={run_date}/primary/"

print(f"Scanning for SD Beach WQ CSVs under: s3://{bucket_name}/{sd_wq_prefix}")

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=sd_wq_prefix)

all_spark_dfs = []

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            
            if key.endswith('.csv'):
                full_s3_path = f"s3://{bucket_name}/{key}"
                print(f"\nProcessing WQ File: {full_s3_path}")
                
                raw_sdf, found_features = process_sd_beach_csv(full_s3_path)
                
                if raw_sdf is None:
                    continue
                    
                processed_df = apply_sd_wq_pipeline(raw_sdf, found_features)
                all_spark_dfs.append(processed_df)

# ---------------------------------------------------------
# 5. Union and Save
# ---------------------------------------------------------
if all_spark_dfs:
    print(f"\nSuccessfully processed {len(all_spark_dfs)} files. Unioning datasets...")
    
    combined_wq_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_spark_dfs)
    
    print("Writing combined dataset to Delta table...")
    combined_wq_df.write.format("delta").mode("overwrite").saveAsTable("default.sd_beach_wq_tiled_all")
    display(combined_wq_df)
    print("Done!")
else:
    print("No valid SD Beach Water Quality data was processed.")

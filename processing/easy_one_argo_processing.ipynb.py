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
import io
import tarfile
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

def process_argo_tarball_csv(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_tar = f"/tmp/{key.split('/')[-1]}"
    
    print(f"  -> Downloading {local_tar}...")
    s3.download_file(bucket_name, key, local_tar)
    
    all_pdfs = []
    global_features = set()
    
    try:
        with tarfile.open(local_tar, "r:gz") as tar:
            csv_members = [
                m for m in tar.getmembers() 
                if m.name.endswith('.csv') and '/data/' in m.name.replace('\\', '/')
            ]
            print(f"  -> Found {len(csv_members)} CSV files. Extracting and parsing metadata...")
            
            # Counter for logging progress on massive archives
            processed_count = 0 
            
            for member in csv_members:
                try:
                    f = tar.extractfile(member)
                    if f is None: continue
                    
                    content = f.read().decode('utf-8', errors='ignore').splitlines()
                    
                    lat, lon, date_str = None, None, None
                    data_lines = []
                    
                    for line in content:
                        if line.startswith('#'):
                            if line.startswith('#profile_latitude'):
                                lat = float(line.split(' ')[1])
                            elif line.startswith('#profile_longitude'):
                                lon = float(line.split(' ')[1])
                            elif line.startswith('#profile_date'):
                                raw_date = line.split(' ')[1][:10]
                                date_str = pd.to_datetime(raw_date).strftime('%m/%d/%Y')
                        else:
                            data_lines.append(line)
                            
                    if not data_lines or lat is None or lon is None or date_str is None:
                        continue
                        
                    csv_io = io.StringIO("\n".join(data_lines))
                    pdf = pd.read_csv(csv_io)
                    
                    # Clean headers: remove units inside parens, lowercase, replace spaces
                    pdf.columns = [c.split(' (')[0].lower().strip().replace(' ', '_') for c in pdf.columns]
                    
                    # Drop duplicated columns safely
                    pdf = pdf.loc[:, ~pdf.columns.duplicated()]
                    
                    pdf['Lat_Dec'] = float(lat)
                    pdf['Lon_Dec'] = float(lon)
                    pdf['conv_date_str'] = date_str
                    
                    # Dynamically collect all numeric features
                    numeric_cols = pdf.select_dtypes(include=['number']).columns.tolist()
                    vars_to_keep = [c for c in numeric_cols if c not in ['lat_dec', 'lon_dec']]
                    
                    if not vars_to_keep:
                        continue 
                        
                    # Add discovered features to our global set
                    global_features.update(vars_to_keep)
                        
                    keep_cols = ['Lat_Dec', 'Lon_Dec', 'conv_date_str'] + vars_to_keep
                    clean_pdf = pdf[keep_cols].dropna(subset=vars_to_keep, how='all')
                    
                    all_pdfs.append(clean_pdf)
                    
                    processed_count += 1
                    if processed_count % 100000 == 0:
                        print(f"  -> Processed {processed_count} files...")
                            
                except Exception as e:
                    pass # Skip corrupted CSVs
                    
    finally:
        if os.path.exists(local_tar):
            os.remove(local_tar)
            
    if not all_pdfs:
        return None, []
        
    # Concat all dataframes. Columns that don't exist in some CSVs will be filled with NaN
    combined_pdf = pd.concat(all_pdfs, ignore_index=True)
    
    # Safe Iteration for dtype casting
    for col, col_dtype in combined_pdf.dtypes.items():
        if col_dtype == 'object':
            combined_pdf[col] = combined_pdf[col].astype(str)
        elif 'float' in str(col_dtype):
            combined_pdf[col] = combined_pdf[col].astype(float)
            
    return spark.createDataFrame(combined_pdf), list(global_features)

# COMMAND ----------

def apply_argo_tiling_pipeline(df, feature_cols):
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
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
argo_prefix = f"sources/easyoneargo/raw/run_date={run_date}/"

print(f"Scanning for Tarballs under: s3://{bucket_name}/{argo_prefix}")

paginator = s3.get_paginator('list_objects_v2')
pages = paginator.paginate(Bucket=bucket_name, Prefix=argo_prefix)

all_spark_dfs = []

for page in pages:
    if 'Contents' in page:
        for obj in page['Contents']:
            key = obj['Key']
            
            if key.endswith('.tar.gz'):
                full_s3_path = f"s3://{bucket_name}/{key}"
                print(f"\nProcessing Archive: {full_s3_path}")
                
                raw_sdf, found_features = process_argo_tarball_csv(full_s3_path)
                
                if raw_sdf is None:
                    print(f"  -> Skipping (no valid target data found)")
                    continue
                    
                processed_df = apply_argo_tiling_pipeline(raw_sdf, found_features)
                all_spark_dfs.append(processed_df)

# ---------------------------------------------------------
# 5. Union and Save
# ---------------------------------------------------------
if all_spark_dfs:
    print(f"\nSuccessfully processed {len(all_spark_dfs)} tarballs. Unioning datasets...")
    
    # Due to dynamic schemas between tarballs, unionByName with allowMissingColumns=True is required
    combined_argo_df = reduce(lambda df1, df2: df1.unionByName(df2, allowMissingColumns=True), all_spark_dfs)
    
    print("Writing combined dataset to Delta table...")
    combined_argo_df.write.format("delta").mode("overwrite").saveAsTable("default.argo_tiled_all")
    print("Done!")
else:
    print("No valid Argo data was processed.")

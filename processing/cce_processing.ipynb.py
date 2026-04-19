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
# ]
# ///
import os
from dotenv import load_dotenv
import boto3

# 1. Load the .env file
load_dotenv()

s3_base_path = os.getenv('raw_bucket_name') 
s3_base_path = f"s3://{s3_base_path.split(':::')[-1]}/noaa_cce1/"

# 2. Explicitly set the environment variables for the current session
# This is what boto3 and fsspec look for
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('aws_secret_access_key')

# Optional: Set the region if your bucket is not in us-east-1
os.environ['AWS_DEFAULT_REGION'] = os.getenv('aws_region', 'us-west-2')

# 3. Test the connection with boto3
s3 = boto3.client('s3')
try:
    s3.list_objects_v2(Bucket="toxictide-raw", MaxKeys=1)
    print("Credentials successfully located and verified.")
except Exception as e:
    print(f"Still failing: {e}")

# COMMAND ----------

import os
import boto3
import xarray as xr
import pandas as pd
from pyspark.sql import SparkSession, functions as F

def process_noaa_nc_local(s3_path):
    spark = SparkSession.builder.getOrCreate()
    
    # 1. Path and Download logic
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_filename = f"/tmp/{key.split('/')[-1]}"
    
    s3 = boto3.client('s3')
    s3.download_file(bucket_name, key, local_filename)
    
    try:
        # 2. Open Dataset
        with xr.open_dataset(local_filename, engine='netcdf4') as ds:
            
            # Helper to safely extract a single float from attributes or arrays
            def get_coord(ds, name):
                # Try attributes first, then variables
                val = ds.attrs.get(name, ds.get(name))
                if val is None: return 0.0
                try:
                    # If it's an xarray/numpy array, flatten and take the first element
                    return float(val.values.flatten()[0]) if hasattr(val, 'values') else float(val)
                except:
                    return 0.0

            lat_val = get_coord(ds, 'LATITUDE')
            lon_val = get_coord(ds, 'LONGITUDE')
            
            # 3. Standardize casing to lowercase
            ds = ds.rename({v: v.lower() for v in ds.variables})
            
            # 4. Flatten to Pandas
            pdf = ds.to_dataframe().reset_index()
            
            # 5. Coordinate and Date Standardization
            # We force Lat_Dec/Lon_Dec to ensure they match your CalCOFI column names
            pdf['Lat_Dec'] = lat_val
            pdf['Lon_Dec'] = lon_val
            
            if 'time' in pdf.columns:
                pdf['conv_date_str'] = pd.to_datetime(pdf['time']).dt.strftime('%m/%d/%Y')
            
            # 6. Spark Compatibility Cleanup
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
    # 1. Date Conversion
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
    # 2. Manual Scaling Loop
    # We calculate stats for all features found in this specific NetCDF
    stats = df.select([F.mean(c).alias(f"{c}_avg") for c in feature_cols] + 
                      [F.stddev(c).alias(f"{c}_std") for c in feature_cols]).collect()[0]
    
    scaled_names = []
    for c in feature_cols:
        avg, std = stats[f"{c}_avg"], stats[f"{c}_std"]
        col_name = f"{c}_scaled"
        # Standard Z-score: (x - μ) / σ
        df = df.withColumn(col_name, F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0)))
        scaled_names.append(col_name)

    # 3. Universal 500m Tiling
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("Lat_Dec") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("Lon_Dec") * (M_PER_DEG * F.cos(F.radians(F.col("Lat_Dec")))) / TILE_SIZE
        )
    )
    
    # 4. Final State: Date, Lon, Lat, Tile_ID, and Feature Array
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

# 1. Anchor the Spark Session (Run this first to avoid SystemError)
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()

# 2. Define the Target File (Pick a known SEACAT file from your list)
test_s3_path = f"s3://toxictide-raw/noaa_cce1/OS_CCE1_01_D_SEACAT.nc"

# 3. Run the Extraction
# This uses the local buffer logic to bypass s3fs conflicts
print(f"Testing extraction for: {test_s3_path}")
raw_sdf, feature_vars = process_noaa_nc_local(test_s3_path)
raw_sdf.columns

# COMMAND ----------

# 1. Run the updated extraction
raw_sdf, feature_vars = process_noaa_nc_local(test_s3_path)

# 2. These will now be lowercase due to our rename step in the function
relevant_features = [v for v in feature_vars if v in ['temp', 'psal', 'depth', 'cndc']]

# 3. This should now find 'conv_date_str' correctly
final_state_df = apply_noaa_tiling_pipeline(raw_sdf, relevant_features)

# Make sure to include tile_id in your final selection!
output = final_state_df.select(
    "tile_id",
    "conventional_date",
    "Lon_Dec",
    "Lat_Dec",
    "environmental_features" 
)

display(output)

# Write to a Delta table (replace database/table names as needed)
output.write.format("delta").mode("overwrite").saveAsTable("default.cce_tiled")

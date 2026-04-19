# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# dependencies = [
#   "python-dotenv",
#   "numpy<2",
# ]
# ///
from dotenv import load_dotenv
import os
load_dotenv()

s3_base_path = os.getenv('raw_bucket_name') 
bottle_s3_path = f"s3://{s3_base_path.split(':::')[-1]}/cal_cofi/194903-202105_Bottle.csv"
cast_s3_path = f"s3://{s3_base_path.split(':::')[-1]}/cal_cofi/194903-202105_Cast.csv"

os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('aws_secret_access_key')
os.environ['AWS_DEFAULT_REGION'] = os.getenv('aws_region', 'us-west-2')

# COMMAND ----------

import boto3
import pandas as pd
import io

# Initialize the S3 client using the env vars you already set
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('aws_access_key_id'),
    aws_secret_access_key=os.getenv('aws_secret_access_key'),
    region_name=os.getenv('aws_region', 'us-west-2')
)

def s3_to_spark(bucket, key):
    # Fetch the object from S3
    obj = s3_client.get_object(Bucket=bucket, Key=key)
    
    # Use 'latin1' encoding to handle the micro (µ) symbol
    pdf = pd.read_csv(
        io.BytesIO(obj['Body'].read()), 
        low_memory=False, 
        encoding='latin1'
    )
    
    return spark.createDataFrame(pdf)

# Usage
bucket_name = s3_base_path.split(':::')[-1]
bottle_df = s3_to_spark(bucket_name, "cal_cofi/194903-202105_Bottle.csv")
cast_df = s3_to_spark(bucket_name, "cal_cofi/194903-202105_Cast.csv")

# 2. Select only the features you need from the Cast table 
# This keeps the join operation performant
cast_cols = ["Cst_Cnt", "Lat_Dec", "Lon_Dec", "Date"]
cast_subset = cast_df.select(*cast_cols)

# 3. Create the single DataFrame via an Inner Join
# 'Cst_Cnt' is the primary key in Cast and the foreign key in Bottle
df = bottle_df.join(cast_subset, on="Cst_Cnt", how="inner")

# Now your 'df' contains both the chemical data and the spatial/date data
print(f"Combined DataFrame Columns: {df.columns}")

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Feature Categorization
spatial_features = ["Lon_Dec", "Lat_Dec"]
# These are the "all other features associated"
chemical_features = [
    "Depthm", "T_degC", "Salnty", "O2ml_L", "STheta", 
    "O2Sat", "ChlorA", "PO4uM", "SiO3uM", "NO2uM", "NO3uM"
]

# 2. Imputation & Schema Prep (Assuming df_imputed is ready)
# Ensure spatial coordinates are handled for the 'if available' logic
df_prep = df.withColumn("conventional_date", F.to_date(F.col("Date"), "MM/dd/yyyy"))

# 3. Scaling the Environmental Features
# We only scale the chemical_features; we keep Lat/Long raw for tiling
stats = df_prep.select([
    F.mean(c).alias(f"{c}_mean") for c in chemical_features
] + [
    F.stddev(c).alias(f"{c}_stddev") for c in chemical_features
]).collect()[0]

df_scaled = df_prep
scaled_chemical_names = []

for c in chemical_features:
    mean_val = stats[f"{c}_mean"]
    std_val = stats[f"{c}_stddev"]
    
    col_name = f"{c}_scaled"
    if std_val == 0 or std_val is None:
        df_scaled = df_scaled.withColumn(col_name, F.lit(0.0))
    else:
        df_scaled = df_scaled.withColumn(col_name, (F.col(c) - mean_val) / std_val)
    scaled_chemical_names.append(col_name)

# 4. Create the "Join Anchor" for Fallback Logic
# This creates a tile ID if Lat/Long exist, otherwise uses the Date string.
precision = 10  # 0.1 degree tiles (~11km)
df_final = df_scaled.withColumn(
    "tile_id",
    F.when(F.col("Lat_Dec").isNotNull() & F.col("Lon_Dec").isNotNull(),
           F.concat(
               F.round(F.col("Lat_Dec") * precision) / precision,
               F.lit("_"),
               F.round(F.col("Lon_Dec") * precision) / precision
           )
    ).otherwise(F.lit(None))
)

# 5. Assemble "Other Features" into an Array
df_final = df_final.withColumn("environmental_features", F.array(*scaled_chemical_names))

# 6. Final Selection
# Separate: Date, Lon, Lat, and the Feature Array
output = df_final.select(
    "conventional_date",
    "Lon_Dec",
    "Lat_Dec",
    "environmental_features"
)

display(output)

# COMMAND ----------

from pyspark.sql import functions as F

# 1. Tiling Constants
TILE_SIZE_METERS = 500 
METERS_PER_DEGREE = 111320

# 2. Universal Tiling Logic
# We calculate the number of tiles per degree for both Lat and Lon
df_tiled = df_final.withColumn(
    "lat_tile_idx", 
    F.floor(F.col("Lat_Dec") * METERS_PER_DEGREE / TILE_SIZE_METERS)
).withColumn(
    "lon_tile_idx", 
    F.floor(
        F.col("Lon_Dec") * (METERS_PER_DEGREE * F.cos(F.radians(F.col("Lat_Dec")))) / TILE_SIZE_METERS
    )
)

# 3. Create the Spatial/Temporal Key
# This handles the "if available" logic: 
# It creates a Tile ID if coordinates exist, otherwise it's null.
df_final = df_tiled.withColumn(
    "tile_id",
    F.when(
        F.col("Lat_Dec").isNotNull() & F.col("Lon_Dec").isNotNull(),
        F.concat_ws("_", F.col("lat_tile_idx"), F.col("lon_tile_idx"))
    ).otherwise(F.lit(None))
)

# 4. Final Output State
# This matches your requested format: Date, Lon, Lat, and the associated Features
output = df_final.select(
    "conventional_date",
    "Lon_Dec",
    "Lat_Dec",
    "tile_id",
    "environmental_features"
)

output.write.format("delta").mode("overwrite").saveAsTable("default.cal_cofi_tiled")

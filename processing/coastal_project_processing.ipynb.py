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
import io
import re
import boto3
import pandas as pd
from pyspark.sql import SparkSession, functions as F

def process_seabass_local(s3_path):
    spark = SparkSession.builder.getOrCreate()
    
    # 1. Download as Bytes first to handle encoding weirdness
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    
    s3 = boto3.client('s3')
    response = s3.get_object(Bucket=bucket_name, Key=key)
    raw_data = response['Body'].read()
    
    # Try decoding with 'latin-1' (it never fails, unlike utf-8)
    content = raw_data.decode('latin-1', errors='ignore')

    # 2. Split the file at /end_header using a robust Regex
    # This finds the marker even if there are spaces, tabs, or \r around it
    parts = re.split(r'/end_header', content, flags=re.IGNORECASE)
    
    if len(parts) < 2:
        # Last ditch effort: try without the slash
        parts = re.split(r'end_header', content, flags=re.IGNORECASE)
        if len(parts) < 2:
            print(f"START OF FILE CONTENT: {content[:500]}")
            raise ValueError(f"CRITICAL: Header termination marker not found in {s3_path}")

    header_text = parts[0]
    data_text = parts[1]

    # 3. Extract Metadata from Header
    # Look for /fields=...
    fields_match = re.search(r'/fields\s*=\s*(.*)', header_text, re.IGNORECASE)
    if not fields_match:
        raise ValueError("Could not find /fields in SeaBASS header.")
    fields = [f.strip() for f in fields_match.group(1).split(',')]

    # Look for Global Lat/Lon
    lat_match = re.search(r'/(north_)?latitude\s*=\s*([-+]?\d*\.\d+|\d+)', header_text, re.IGNORECASE)
    lon_match = re.search(r'/(west_)?longitude\s*=\s*([-+]?\d*\.\d+|\d+)', header_text, re.IGNORECASE)
    lat_meta = float(lat_match.group(2)) if lat_match else 0.0
    lon_meta = float(lon_match.group(2)) if lon_match else 0.0

    # 4. Clean Data Section
    # Remove any trailing metadata lines that start with '/' or comments '!'
    clean_lines = [l for l in data_text.splitlines() if l.strip() and not l.strip().startswith(('/', '!'))]
    
    # 5. Load into Pandas
    pdf = pd.read_csv(
        io.StringIO("\n".join(clean_lines)),
        names=fields,
        sep=r'\s+',
        engine='python'
    )

    # 6. Standardize Coordinates (As requested: Capitalized)
    pdf['LATITUDE'] = pdf['latitude'].astype(float) if 'latitude' in pdf.columns else lat_meta
    pdf['LONGITUDE'] = pdf['longitude'].astype(float) if 'longitude' in pdf.columns else lon_meta

    # Handle Date
    if 'date' in pdf.columns:
        pdf['conv_date_str'] = pd.to_datetime(pdf['date'], format='%Y%m%d', errors='coerce').dt.strftime('%m/%d/%Y')
    else:
        pdf['conv_date_str'] = "01/01/2000" # Fallback

    # Feature List
    exclude = ['date', 'time', 'latitude', 'longitude', 'lat', 'lon', 'depth']
    feature_vars = [f for f in fields if f.lower() not in exclude]

    # Spark Type Alignment
    for col in pdf.columns:
        if 'float' in str(pdf[col].dtype) or 'int' in str(pdf[col].dtype):
            pdf[col] = pdf[col].astype(float)
        else:
            pdf[col] = pdf[col].astype(str)

    return spark.createDataFrame(pdf), feature_vars

# COMMAND ----------

# 1. Execute the Parser
test_file = "s3://toxictide-raw/ob_daac/0_San_Diego_Coastal_Project_e1f626210c_I2001_casb.sb"
raw_sdf, feature_vars = process_seabass_local(test_file)

# 2. Apply Tiling (0.5km Resolution)
def apply_seabass_tiling(df, feature_cols):
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    # Universal 500m Tiling logic
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("LATITUDE") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("LONGITUDE") * (M_PER_DEG * F.cos(F.radians(F.col("LATITUDE")))) / TILE_SIZE
        )
    )
    
    # Create tile_id and Feature Array
    df_final = df_tiled.withColumn(
        "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
    ).withColumn(
        "environmental_features", F.array(*[F.col(c) for c in feature_cols])
    )
    
    return df_final.select("conventional_date", "LONGITUDE", "LATITUDE", "tile_id", "environmental_features")

final_output = apply_seabass_tiling(raw_sdf, feature_vars)
display(final_output)

# COMMAND ----------

# Run this to see what is ACTUALLY in the file
s3 = boto3.client('s3')
obj = s3.get_object(Bucket="toxictide-raw", Key="ob_daac/0_San_Diego_Coastal_Project_e1f626210c_I2001_casb.sb")
raw_peek = obj['Body'].read(1000)
print(f"RAW BYTES PEEK: {raw_peek}")

# COMMAND ----------

# COMMAND ----------
# Initialize test
test_file = "s3://toxictide-raw/ob_daac/0_San_Diego_Coastal_Project_e1f626210c_I2001_casb.sb"
raw_sdf, feature_vars = process_seabass_local(test_file)

# Use your 500m Tiling Pipeline
output = apply_seabass_tiling(raw_sdf, feature_vars)
display(output)

# COMMAND ----------

# --- TEST CASE: San Diego Coastal Project ---
test_file = "s3://toxictide-raw/ob_daac/0_San_Diego_Coastal_Project_e1f626210c_I2001_casb.sb"

raw_sdf, feature_vars = process_seabass_local(test_file)

# Re-use your universal tiling logic
def apply_seabass_tiling(df, feature_cols):
    df = df.withColumn("conventional_date", F.to_date(F.col("conv_date_str"), "MM/dd/yyyy"))
    
    # Tiling Constants
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    # Universal 500m Tiling (using your capitalized LATITUDE/LONGITUDE)
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("LATITUDE") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("LONGITUDE") * (M_PER_DEG * F.cos(F.radians(F.col("LATITUDE")))) / TILE_SIZE
        )
    )
    
    # Final Output State
    df_final = df_tiled.withColumn(
        "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
    ).withColumn(
        "environmental_features", F.array(*[F.col(c) for c in feature_cols])
    )
    
    return df_final.select("conventional_date", "LONGITUDE", "LATITUDE", "tile_id", "environmental_features")

output = apply_seabass_tiling(raw_sdf, feature_vars)
display(output)

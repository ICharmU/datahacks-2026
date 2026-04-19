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
from pyspark.sql import functions as F

# 1. Read the generated outputs
df_cal_cofi = spark.read.table("default.cal_cofi_tiled")
df_cce = spark.read.table("default.cce_tiled")

# 2. Rename ALL conflicting columns BEFORE joining
# This prevents duplicate column errors for Lat/Lon as well as your features
df_cal_cofi = df_cal_cofi \
    .withColumnRenamed("environmental_features", "calcofi_env_features") \
    .withColumnRenamed("Lon_Dec", "calcofi_Lon_Dec") \
    .withColumnRenamed("Lat_Dec", "calcofi_Lat_Dec") \
    .withColumnRenamed("conventional_date", "calcofi_date")

df_cce = df_cce \
    .withColumnRenamed("environmental_features", "cce_env_features") \
    .withColumnRenamed("Lon_Dec", "cce_Lon_Dec") \
    .withColumnRenamed("Lat_Dec", "cce_Lat_Dec") \
    .withColumnRenamed("conventional_date", "cce_date")

# 3. Perform the join
merged_df = df_cal_cofi.join(
    df_cce,
    on=["tile_id"], 
    how="inner"
)

# 4. Handle raw Lat/Lon using the safely renamed columns
merged_df = merged_df.withColumn(
    "Lon_Dec", F.coalesce(F.col("calcofi_Lon_Dec"), F.col("cce_Lon_Dec"))
).withColumn(
    "Lat_Dec", F.coalesce(F.col("calcofi_Lat_Dec"), F.col("cce_Lat_Dec"))
)

# 5. Clean up by dropping the prefixed source coordinates
merged_df = merged_df.drop(
    "calcofi_Lon_Dec", "cce_Lon_Dec", 
    "calcofi_Lat_Dec", "cce_Lat_Dec"
)

# 6. Save the final merged dataset

# overwrite schema if needing to rename, otherwise don't change schema
# merged_df.write \
#     .format("delta") \
#     .mode("overwrite") \
#     .option("overwriteSchema", "true")\ # 
#     .saveAsTable("default.merged")

merged_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("default.merged")

# COMMAND ----------

# 1. Read the Delta table into a PySpark DataFrame
df_merged = spark.read.table("default.merged")

# 2. View the DataFrame 
display(df_merged)

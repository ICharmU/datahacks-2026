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
import tarfile
import os
import boto3
import xarray as xr
from dotenv import load_dotenv

load_dotenv()
os.environ['AWS_ACCESS_KEY_ID'] = os.getenv('aws_access_key_id')
os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv('aws_secret_access_key')

def peek_argo_tarball(s3_path):
    path_parts = s3_path.replace("s3://", "").split("/")
    bucket_name = path_parts[0]
    key = "/".join(path_parts[1:])
    local_tar = f"/tmp/{key.split('/')[-1]}"
    
    s3 = boto3.client('s3')
    print(f"Downloading {s3_path}...")
    s3.download_file(bucket_name, key, local_tar)
    
    with tarfile.open(local_tar, "r:gz") as tar:
        # List first 10 files to see the internal structure
        members = tar.getmembers()
        print(f"Total files in tarball: {len(members)}")
        
        # Find the first NetCDF profile file (usually starts with 'R' or 'D')
        nc_files = [m for m in members if m.name.endswith('.nc')]
        if not nc_files:
            return "No NetCDF files found in tarball."
        
        sample_member = nc_files[0]
        print(f"Extracting sample: {sample_member.name}")
        tar.extract(sample_member, path="/tmp/argo_sample/")
        
        sample_path = f"/tmp/argo_sample/{sample_member.name}"
        
        # Open and inspect
        with xr.open_dataset(sample_path) as ds:
            return ds.to_dataframe().columns.tolist(), ds.attrs
            
# --- Run Peek ---
argo_sample_path = "s3://toxictide-raw/easy_one_argo/argo_127234.tar.gz"
columns, metadata = peek_argo_tarball(argo_sample_path)

print("\n--- Argo Columns Found ---")
for col in columns:
    print(col)

# COMMAND ----------



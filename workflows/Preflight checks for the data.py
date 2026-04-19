# Databricks notebook source
print("Databricks S3 sanity notebook started")
print("Current catalog / schema checks will follow")

# COMMAND ----------

raw_calcofi = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load("s3://toxictide-raw/sources/calcofi/raw/")
)

raw_cce = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load("s3://toxictide-raw/sources/cce_moorings/raw/")
)

raw_argo = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load("s3://toxictide-raw/sources/easyoneargo/raw/")
)

raw_ca_beach = (
    spark.read.format("binaryFile")
    .option("recursiveFileLookup", "true")
    .load("s3://toxictide-raw/sources/ca_beach_water_quality/raw/")
)

# COMMAND ----------

display(raw_calcofi.select("path", "length", "modificationTime"))
display(raw_cce.select("path", "length", "modificationTime"))
display(raw_argo.select("path", "length", "modificationTime"))
display(raw_ca_beach.select("path", "length", "modificationTime"))

# COMMAND ----------

print("CalCOFI files:", raw_calcofi.count())
print("CCE Moorings files:", raw_cce.count())
print("EasyOneArgo files:", raw_argo.count())
print("CA beach files:", raw_ca_beach.count())

# COMMAND ----------

ca_beach_csv = (
    spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("recursiveFileLookup", "true")
    .load("s3://toxictide-raw/sources/ca_beach_water_quality/raw/")
)
display(ca_beach_csv.limit(20))
print("Columns:", ca_beach_csv.columns)

# COMMAND ----------

manifest_files = spark.read.format("binaryFile").load("s3://toxictide-raw/sources/*/manifests/*.json")
display(manifest_files.select("path", "length", "modificationTime"))

# COMMAND ----------

def ls_recursive(path: str, max_depth: int = 3, depth: int = 0):
    if depth > max_depth:
        return
    try:
        entries = dbutils.fs.ls(path)
    except Exception as e:
        print("ERR", path, "->", e)
        return

    indent = "  " * depth
    for e in entries:
        print(f"{indent}{e.path}")
        if e.path.endswith("/"):
            ls_recursive(e.path, max_depth=max_depth, depth=depth + 1)

# COMMAND ----------

ls_recursive("s3://toxictide-raw/sources/", max_depth=4)

# COMMAND ----------

display(dbutils.fs.ls("s3://toxictide-raw/sources/"))
display(dbutils.fs.ls("s3://toxictide-raw/sources/ca_beach_water_quality/"))
display(dbutils.fs.ls("s3://toxictide-raw/sources/ca_beach_water_quality/raw/"))

# COMMAND ----------

sources = [
    "calcofi",
    "cce_moorings",
    "easyoneargo",
    "ca_beach_water_quality",
]

for src in sources:
    print(f"\n=== {src} ===")
    try:
        display(dbutils.fs.ls(f"s3://toxictide-raw/sources/{src}/"))
    except Exception as e:
        print("top-level error:", e)

# COMMAND ----------

for src in sources:
    print(f"\n=== {src} raw recursive inventory ===")
    try:
        df = (
            spark.read.format("binaryFile")
            .option("recursiveFileLookup", "true")
            .load(f"s3://toxictide-raw/sources/{src}/raw/")
        )
        print("count =", df.count())
        display(df.select("path", "length", "modificationTime"))
    except Exception as e:
        print("inventory error:", e)

# COMMAND ----------


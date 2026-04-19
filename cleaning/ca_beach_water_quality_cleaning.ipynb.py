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
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

# Initialize Spark
spark = SparkSession.builder.getOrCreate()

# Tables
RAW_TABLE = "default.ca_beach_wq_raw"
PROCESSED_TABLE = "default.ca_beach_wq_processing"

# The slugified features from our ingestion pipeline
TARGET_FEATURES = ["enterococcus", "total_coliform", "fecal_coliform"]

# COMMAND ----------

def process_water_quality_pipeline():
    print(f"Loading raw data from {RAW_TABLE}...")
    df = spark.read.table(RAW_TABLE)
    
    # ---------------------------------------------------------
    # Phase 1: Feature Pruning
    # ---------------------------------------------------------
    total_rows = df.count()
    print(f"Scanning {total_rows} rows for 100% null columns...")
    
    # Calculate non-null counts for all columns
    null_counts = df.select([
        F.count(F.when(F.col(c).isNotNull(), c)).alias(c) 
        for c in df.columns
    ]).collect()[0].asDict()
    
    useless_cols = [c for c, val in null_counts.items() if val == 0]
    df = df.drop(*useless_cols)
    print(f"Dropped {len(useless_cols)} useless columns.")

    # ---------------------------------------------------------
    # Phase 2: Biological Interpolation (LOCF)
    # ---------------------------------------------------------
    # Define a window: Same station, ordered by date.
    # We look back 3 days to fill gaps.
    # Note: We use 'rangeBetween' on the unix timestamp to ensure it's exactly 3 days.
    DAYS_3_SECONDS = 3 * 24 * 60 * 60
    
    # Create a numeric timestamp for the range window
    df = df.withColumn("ts", F.unix_timestamp("conventional_date"))
    
    window_spec = Window.partitionBy("station_id").orderBy("ts") \
                        .rangeBetween(-DAYS_3_SECONDS, 0)
    
    print("Interpolating bacteria counts (3-day constrained forward-fill)...")
    for feat in TARGET_FEATURES:
        if feat in df.columns:
            # Forward fill using the last known value in the 3-day window
            df = df.withColumn(feat, F.last(F.col(feat), ignoreNulls=True).over(window_spec))

    # ---------------------------------------------------------
    # Phase 3: Log-Transformation & Scaling
    # ---------------------------------------------------------
    print("Applying log-transformation and calculating global Z-scores...")
    
    # Log transform to handle exponential bacteria growth: log10(x + 1)
    for feat in TARGET_FEATURES:
        if feat in df.columns:
            df = df.withColumn(feat, F.log10(F.col(feat) + 1.0))

    # Calculate global Mean and StdDev for the log-transformed features
    stats = df.select(
        [F.mean(c).alias(f"{c}_avg") for c in TARGET_FEATURES] + 
        [F.stddev(c).alias(f"{c}_std") for c in TARGET_FEATURES]
    ).collect()[0]
    
    scaled_names = []
    for c in TARGET_FEATURES:
        avg = stats[f"{c}_avg"]
        std = stats[f"{c}_std"]
        col_name = f"{c}_scaled"
        
        # Standard Scaling: (x - mu) / sigma
        df = df.withColumn(
            col_name, 
            F.when(F.lit(std) > 0, (F.col(c) - avg) / std).otherwise(F.lit(0.0))
        )
        scaled_names.append(col_name)

    # ---------------------------------------------------------
    # Phase 4: 500m Tiling
    # ---------------------------------------------------------
    print("Generating 500m tiles...")
    TILE_SIZE = 500 
    M_PER_DEG = 111320
    
    df_tiled = df.withColumn(
        "lat_idx", F.floor(F.col("lat_dec") * M_PER_DEG / TILE_SIZE)
    ).withColumn(
        "lon_idx", F.floor(
            F.col("lon_dec") * (M_PER_DEG * F.cos(F.radians(F.col("lat_dec")))) / TILE_SIZE
        )
    )
    
    df_final = df_tiled.withColumn(
        "tile_id", F.concat_ws("_", F.col("lat_idx"), F.col("lon_idx"))
    ).withColumn(
        "environmental_features", F.array(*[F.col(c).cast(DoubleType()) for c in scaled_names])
    )

    # ---------------------------------------------------------
    # Phase 5: Final Output
    # ---------------------------------------------------------
    output = df_final.select(
        "tile_id", 
        "conventional_date", 
        "lon_dec", 
        "lat_dec", 
        "environmental_features"
    ).dropna(subset=["environmental_features"])

    print(f"Writing processed data to {PROCESSED_TABLE}...")
    output.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(PROCESSED_TABLE)
    print("Pipeline execution successful.")

# Run the pipeline
process_water_quality_pipeline()

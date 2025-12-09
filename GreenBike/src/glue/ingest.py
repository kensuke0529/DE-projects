import sys
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

s3 = boto3.client("s3")

BUCKET = "greenbike-data"
INFO_ROOT = "raw/info/"
STATUS_ROOT = "raw/status/"

# -------- Helper Functions -------- #


def list_files(prefix):
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".json")]


def get_files_since_last_run(prefix, hours_back=4):
    """Get all files from the last N hours to process incremental data"""
    files = list_files(prefix)
    if not files:
        raise Exception(f"No JSON files in {prefix}")
    
    # Sort by numeric file name like 211809.json (HHMMSS format)
    sorted_files = sorted(files, key=lambda x: int(x.split("/")[-1].split(".")[0]))
    
    # Get files from the last N hours (to catch all Lambda runs since last Glue run)
    # Since Lambda runs every 15 min, 4 hours = 16 files
    cutoff_time = datetime.utcnow() - timedelta(hours=hours_back)
    
    recent_files = []
    for file_key in sorted_files:
        # Parse timestamp from path: raw/status/2025/12/04/211809.json
        parts = file_key.split("/")
        if len(parts) >= 5:
            try:
                year, month, day = int(parts[2]), int(parts[3]), int(parts[4])
                time_str = parts[5].split(".")[0]  # e.g., "211809"
                hour, minute = int(time_str[:2]), int(time_str[2:4])
                file_time = datetime(year, month, day, hour, minute)
                
                if file_time >= cutoff_time:
                    recent_files.append(file_key)
            except (ValueError, IndexError):
                # If parsing fails, include the file to be safe
                recent_files.append(file_key)
    
    # If no recent files, process at least the latest one
    if not recent_files:
        recent_files = [sorted_files[-1]]
    
    return recent_files


# -------- Find files to process -------- #

# Get latest info file (info doesn't change as frequently)
info_latest_key = sorted(list_files(INFO_ROOT), key=lambda x: int(x.split("/")[-1].split(".")[0]))[-1]
info_latest_path = f"s3://{BUCKET}/{info_latest_key}"

# Get all status files from last 4 hours (to catch all Lambda runs)
status_files = get_files_since_last_run(STATUS_ROOT, hours_back=4)
status_paths = [f"s3://{BUCKET}/{key}" for key in status_files]

print("INFO:", info_latest_path)
print(f"STATUS files to process ({len(status_paths)}):", status_paths[:5], "..." if len(status_paths) > 5 else "")

# -------- Load & Flatten -------- #

info_df = (
    spark.read.json(info_latest_path)
    .select(F.explode("data.stations").alias("station"))
    .select("station.*")
)

# Read all status files and union them
status_df = None
for status_path in status_paths:
    df_temp = (
        spark.read.json(status_path)
        .select(F.explode("data.stations").alias("station"))
        .select("station.*")
    )
    if status_df is None:
        status_df = df_temp
    else:
        status_df = status_df.union(df_temp)

# Extract bike types and convert timestamp
status_df = (
    status_df.withColumn("electric", F.col("num_bikes_available_types.electric"))
    .withColumn("classic", F.col("num_bikes_available_types.classic"))
    .withColumn("timestamp", F.from_unixtime(F.col("last_reported")).cast("timestamp"))
    .drop("num_bikes_available_types", "last_reported")
)

# -------- Join -------- #

df = status_df.join(info_df, "station_id", "inner")

# ----- Add partition columns from timestamp (not from file path) -----
df = (
    df.withColumn("year", F.year(F.col("timestamp")))
    .withColumn("month", F.month(F.col("timestamp")))
    .withColumn("day", F.dayofmonth(F.col("timestamp")))
)

# -------- Write to partitioned Parquet -------- #

output_path = "s3://greenbike-data/curated/parquet/"

(df.write.mode("append").partitionBy("year", "month", "day").parquet(output_path))

print(f"Wrote {df.count()} rows to curated partitions at:", output_path)

job.commit()

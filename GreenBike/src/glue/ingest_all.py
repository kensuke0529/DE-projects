import sys
import boto3
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.job import Job
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()

# -------- Cost Optimization: Reduce resource usage -------- #
# These settings reduce DPU usage by limiting parallelism and memory
sc._conf.set("spark.sql.shuffle.partitions", "2")  # Reduce shuffle partitions (default 200)
sc._conf.set("spark.default.parallelism", "2")    # Reduce default parallelism
sc._conf.set("spark.sql.files.maxPartitionBytes", "134217728")  # 128MB partitions (larger = fewer tasks)

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


# -------- Find all files to process -------- #

# Get latest info file (info doesn't change as frequently, so use latest)
info_files = list_files(INFO_ROOT)
if not info_files:
    raise Exception(f"No JSON files in {INFO_ROOT}")

info_latest_key = sorted(info_files, key=lambda x: int(x.split("/")[-1].split(".")[0]))[-1]
info_latest_path = f"s3://{BUCKET}/{info_latest_key}"

# Get ALL status files (for backfilling/processing all historical data)
status_files = list_files(STATUS_ROOT)
if not status_files:
    raise Exception(f"No JSON files in {STATUS_ROOT}")

# Sort by path to process in chronological order
status_files_sorted = sorted(status_files, key=lambda x: (
    int(x.split("/")[2]),  # year
    int(x.split("/")[3]),  # month
    int(x.split("/")[4]),  # day
    int(x.split("/")[5].split(".")[0])  # time (HHMMSS)
))

status_paths = [f"s3://{BUCKET}/{key}" for key in status_files_sorted]

print("INFO:", info_latest_path)
print(f"STATUS files to process (ALL {len(status_paths)} files):")
print(f"  First: {status_paths[0] if status_paths else 'None'}")
print(f"  Last: {status_paths[-1] if status_paths else 'None'}")

# -------- Load & Flatten -------- #

info_df = (
    spark.read.json(info_latest_path)
    .select(F.explode("data.stations").alias("station"))
    .select("station.*")
)

# Read all status files and union them (cost-optimized: process in batches)
print("Reading status files...")
print(f"Cost optimization: Using minimal parallelism to reduce DPU usage")

# Process files in smaller batches to reduce memory pressure
BATCH_SIZE = 20  # Process 20 files at a time to reduce memory usage
status_dfs = []

for i in range(0, len(status_paths), BATCH_SIZE):
    batch = status_paths[i:i+BATCH_SIZE]
    print(f"  Processing batch {i//BATCH_SIZE + 1}/{(len(status_paths)-1)//BATCH_SIZE + 1}: files {i+1}-{min(i+BATCH_SIZE, len(status_paths))}")
    
    # Read batch of files
    batch_dfs = []
    for status_path in batch:
        df_temp = (
            spark.read.json(status_path)
            .select(F.explode("data.stations").alias("station"))
            .select("station.*")
        )
        batch_dfs.append(df_temp)
    
    # Union batch
    if batch_dfs:
        batch_df = batch_dfs[0]
        for df in batch_dfs[1:]:
            batch_df = batch_df.union(df)
        status_dfs.append(batch_df)

# Union all batches
if status_dfs:
    status_df = status_dfs[0]
    for df in status_dfs[1:]:
        status_df = status_df.union(df)
else:
    raise Exception("No status data loaded")

print(f"Total status records loaded: {status_df.count()}")

# Extract bike types and convert timestamp
status_df = (
    status_df.withColumn("electric", F.col("num_bikes_available_types.electric"))
    .withColumn("classic", F.col("num_bikes_available_types.classic"))
    .withColumn("timestamp", F.from_unixtime(F.col("last_reported")).cast("timestamp"))
    .drop("num_bikes_available_types", "last_reported")
)

# -------- Join -------- #

df = status_df.join(info_df, "station_id", "inner")

# ----- Add partition columns from timestamp -----
df = (
    df.withColumn("year", F.year(F.col("timestamp")))
    .withColumn("month", F.month(F.col("timestamp")))
    .withColumn("day", F.dayofmonth(F.col("timestamp")))
)

# -------- Write to partitioned Parquet -------- #

output_path = "s3://greenbike-data/curated/parquet/"

# Cost optimization: Use fewer partitions for writing (reduces small files)
row_count = df.count()
print(f"Writing {row_count} rows to partitioned Parquet...")
print("Cost optimization: Using minimal partitions to reduce write overhead")

(df.write
 .mode("overwrite")
 .option("maxRecordsPerFile", 100000)  # Combine records into fewer files
 .partitionBy("year", "month", "day")
 .parquet(output_path))

print(f"Successfully wrote all data to: {output_path}")
print(f"Partitions created: year/month/day based on timestamp values")

job.commit()


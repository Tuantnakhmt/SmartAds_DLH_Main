from pyspark.sql import SparkSession
import time, sys
from pathlib import Path

spark = SparkSession.builder \
    .appName("Benchmark Hudi Incremental vs Full") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

hudi_path = "/lakehouse/gold/top_domains_by_category"

# Full scan
start_full = time.time()
df_full = spark.read.format("hudi").load(hudi_path)
full_count = df_full.count()
end_full = time.time()

print(f"\n Full Scan - Time: {end_full - start_full:.2f}s | Rows: {full_count}")

#  Fallback: Get commit times from filesystem
commit_dir = Path("/lakehouse/gold/top_domains_by_category/.hoodie")
commit_files = sorted([
    f.name.replace(".commit", "") for f in commit_dir.glob("*.commit")
])

if len(commit_files) < 2:
    print(" Not enough commit files found.")
    sys.exit(1)

commit_time = commit_files[-2]  # second latest

# Incremental scan
start_incr = time.time()
df_incr = spark.read.format("hudi") \
    .option("hoodie.datasource.query.type", "incremental") \
    .option("hoodie.datasource.read.begin.instanttime", commit_time) \
    .load(hudi_path)
incr_count = df_incr.count()
end_incr = time.time()

print(f" Incremental Scan - Time: {end_incr - start_incr:.2f}s | Rows: {incr_count}")
spark.stop()

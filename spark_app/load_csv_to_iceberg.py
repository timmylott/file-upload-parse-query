from pyspark.sql import SparkSession
import os
import sys


file_path = sys.argv[1]      # s3a://uploads/filename.csv or .parquet
table_name = sys.argv[2]     # desired Iceberg table name
file_format = sys.argv[3] if len(sys.argv) > 3 else "PARQUET"  # optional, default PARQUET

spark = (
    SparkSession.builder.appName("csv-to-iceberg")
    .getOrCreate()
)

# Create namespace if it doesn't exist
spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.default")

# Read the file based on extension
if file_path.endswith(".csv"):
    df = spark.read.option("header", "true").csv(file_path)
elif file_path.endswith(".parquet"):
    df = spark.read.parquet(file_path)
else:
    raise ValueError("Unsupported file format. Use CSV or Parquet.")
# Write to Iceberg table
print(f"Writing to iceberg.default.{table_name}")
df.writeTo(f"iceberg.default.{table_name}").createOrReplace()

print("Done.")
spark.stop()

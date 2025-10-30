# /home/iceberg/app/tasks.py
from pyspark.sql import SparkSession
import re

SCHEMA_NAME = "file_uploader"

def sanitize_column_name(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r'[^a-z0-9]', '_', col_name)
    col_name = re.sub(r'_+', '_', col_name)
    col_name = col_name.strip('_')
    return col_name

def process_file(file_path: str, table_name: str):
    """
    Reads CSV or Parquet from S3 (MinIO) and writes to Iceberg.
    """
    spark = SparkSession.builder \
        .appName("RQ-ProcessFileJob") \
        .getOrCreate()

    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{SCHEMA_NAME}")

    if file_path.endswith(".csv"):
        df = spark.read.option("header", "true").csv(file_path)
    elif file_path.endswith(".parquet"):
        df = spark.read.parquet(file_path)
    else:
        raise ValueError("Unsupported file type")

    new_cols = [sanitize_column_name(c) for c in df.columns]
    df = df.toDF(*new_cols)

    df.writeTo(f"iceberg.{SCHEMA_NAME}.{table_name}").createOrReplace()
    print(f"Loaded {file_path} into Iceberg: iceberg.{SCHEMA_NAME}.{table_name}")

    spark.stop()

import os
import time
import json
import subprocess
import re
import boto3
from botocore.client import Config
from pyspark.sql import SparkSession

TRIGGER_DIR = "/shared/triggers"

# MinIO config
MINIO_ENDPOINT = "http://minio:9000"
MINIO_ACCESS_KEY = "admin"
MINIO_SECRET_KEY = "password"
MINIO_BUCKET = "uploads"

SCHEMA_NAME = "file_uploader"

os.makedirs(TRIGGER_DIR, exist_ok=True)

# Setup MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

def sanitize_column_name(col_name):
    col_name = col_name.lower()
    col_name = re.sub(r'[^a-z0-9]', '_', col_name)  # replace non-alphanumeric with _
    col_name = re.sub(r'_+', '_', col_name)         # collapse multiple underscores
    col_name = col_name.strip('_')                  # remove leading/trailing underscores
    return col_name

print("Watcher started — waiting for new trigger files...")

while True:
    for filename in os.listdir(TRIGGER_DIR):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(TRIGGER_DIR, filename)
        try:
            with open(filepath) as f:
                data = json.load(f)

            file_path = data["file_path"]       # e.g., s3a://uploads/file.csv
            table_name = data["table_name"]     # e.g., my_table
            print(f"Trigger found: {file_path} → table {table_name}")

            # Start Spark session
            spark = SparkSession.builder.appName("watcher-load").getOrCreate()

            spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{SCHEMA_NAME}")

            # Read the file
            if file_path.endswith(".csv"):
                df = spark.read.option("header", "true").csv(file_path)
            elif file_path.endswith(".parquet"):
                df = spark.read.parquet(file_path)
            else:
                raise ValueError("Unsupported file type")

            # Sanitize column names
            new_cols = [sanitize_column_name(c) for c in df.columns]
            df = df.toDF(*new_cols)

            # Write to Iceberg
            df.writeTo(f"iceberg.{SCHEMA_NAME}.{table_name}").createOrReplace()
            print(f"Loaded into Iceberg: iceberg.{SCHEMA_NAME}.{table_name}")

            # Move file to processed/
            src_key = file_path.replace("s3a://uploads/", "")
            s3.copy_object(
                Bucket=MINIO_BUCKET,
                CopySource=f"{MINIO_BUCKET}/{src_key}",
                Key=f"processed/{src_key}"
            )
            s3.delete_object(Bucket=MINIO_BUCKET, Key=src_key)
            print("File moved to processed/")

        except Exception as e:
            print(f"Error processing {filename}: {e}")
            try:
                src_key = data.get("file_path", "").replace("s3a://uploads/", "")
                if src_key:
                    s3.copy_object(
                        Bucket=MINIO_BUCKET,
                        CopySource=f"{MINIO_BUCKET}/{src_key}",
                        Key=f"errors/{src_key}"
                    )
                    s3.delete_object(Bucket=MINIO_BUCKET, Key=src_key)
                    print("File moved to errors/")
            except Exception as move_err:
                print(f"Error moving file to errors/: {move_err}")

        finally:
            os.remove(filepath)
            if "spark" in locals():
                spark.stop()

    time.sleep(5)

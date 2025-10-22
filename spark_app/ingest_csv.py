from pyspark.sql import SparkSession
import sys

filename = sys.argv[1]  # e.g., "myfile.csv"
table_name = filename.replace(".csv", "").replace(" ", "_").lower()

spark = SparkSession.builder \
    .appName("CSV to Iceberg") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "s3a://iceberg-warehouse/") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

df = spark.read.option("header", "true").csv(f"s3a://uploads/{filename}")
df.writeTo(f"local.default.{table_name}").using("iceberg").createOrReplace()
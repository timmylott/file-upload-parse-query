from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import boto3
from botocore.client import Config
import os
import re
from datetime import datetime
import redis
from rq import Queue, Retry

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# ----------------------------
# Configuration
# ----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "uploads")

REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))

# ----------------------------
# Redis Queue setup
# ----------------------------
r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
q = Queue("file_tasks", connection=r)

# ----------------------------
# MinIO client setup
# ----------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version="s3v4"),
    region_name="us-east-1",
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
except:
    s3.create_bucket(Bucket=MINIO_BUCKET)


# ----------------------------
# Helper: sanitize filenames
# ----------------------------
def sanitize_filename(filename: str) -> str:
    name, ext = os.path.splitext(filename)
    name = name.lower()
    name = re.sub(r"[^a-z0-9_]", "_", name)
    name = re.sub(r"_+", "_", name)  # collapse multiple underscores
    name = name.strip("_")  # remove leading/trailing underscores
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{name}_{timestamp}{ext}"


# ----------------------------
# Routes
# ----------------------------

@app.get("/", response_class=HTMLResponse)
async def upload_form(request: Request):
    """Show upload form."""
    return templates.TemplateResponse("upload.html", {"request": request, "response": None})


@app.post("/upload/", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)):
    """Upload file to MinIO and enqueue processing job in Redis Queue."""
    try:
        # Read uploaded content
        contents = await file.read()
        sanitized_name = sanitize_filename(file.filename)

        # Upload to MinIO
        s3.put_object(Bucket=MINIO_BUCKET, Key=sanitized_name, Body=contents)
        print(f"Uploaded file to MinIO: {sanitized_name}")

        # Derive Iceberg table name from filename (without extension)
        table_name = os.path.splitext(sanitized_name)[0]

        # Construct job parameters
        file_path = f"s3a://{MINIO_BUCKET}/{sanitized_name}"

        # Enqueue job in Redis Queue using string path
        # "tasks.process_file" should exist in the worker container
        job = q.enqueue("tasks.process_file", file_path, table_name, retry=Retry(max=3))
        print(f"Job enqueued: {job.id}")

        response = {
            "status": "success",
            "uploaded_file": sanitized_name,
            "job_id": job.id,
            "message": "File uploaded and queued for Iceberg ingestion."
        }

    except Exception as e:
        print(f"Upload failed: {e}")
        response = {"status": "error", "message": str(e)}

    return templates.TemplateResponse("upload.html", {"request": request, "response": response})

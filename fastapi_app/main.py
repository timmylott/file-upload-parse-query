from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import boto3
from botocore.client import Config
import os
import re
import json
from datetime import datetime

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# ----------------------------
# Configuration
# ----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "password")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "uploads")

TRIGGER_DIR = "/shared/triggers"
os.makedirs(TRIGGER_DIR, exist_ok=True)

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
    name = re.sub(r'_+', '_', name)         # collapse multiple underscores
    name = name.strip('_')                  # remove leading/trailing underscores
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
    """Upload file to MinIO and create a Spark trigger JSON."""
    try:
        # Read uploaded content
        contents = await file.read()
        sanitized_name = sanitize_filename(file.filename)

        # Upload to MinIO
        s3.put_object(Bucket=MINIO_BUCKET, Key=sanitized_name, Body=contents)
        print(f"Uploaded file to MinIO: {sanitized_name}")

        # Derive Iceberg table name from filename (without extension)
        table_name = os.path.splitext(sanitized_name)[0]

        # Create trigger JSON
        trigger_data = {
            "file_path": f"s3a://{MINIO_BUCKET}/{sanitized_name}",
            "table_name": table_name
        }

        trigger_file = os.path.join(TRIGGER_DIR, f"{table_name}.json")
        with open(trigger_file, "w") as f:
            json.dump(trigger_data, f)

        print(f"Trigger created: {trigger_file}")

        response = {
            "status": "success",
            "uploaded_file": sanitized_name,
            "trigger_file": f"{table_name}.json",
            "message": "File uploaded and trigger created for Spark loader."
        }

    except Exception as e:
        print(f"Upload failed: {e}")
        response = {"status": "error", "message": str(e)}

    return templates.TemplateResponse("upload.html", {"request": request, "response": response})

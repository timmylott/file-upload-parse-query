from fastapi import FastAPI, Request, File, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
import boto3
from botocore.client import Config
import os
import re
from datetime import datetime

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "uploads")

# MinIO client
s3 = boto3.client(
    's3',
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Ensure bucket exists
try:
    s3.head_bucket(Bucket=MINIO_BUCKET)
except:
    s3.create_bucket(Bucket=MINIO_BUCKET)

def sanitize_filename(filename: str) -> str:
    name, ext = os.path.splitext(filename)
    name = name.lower()
    name = re.sub(r'[^a-z0-9_]', '_', name)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"{name}_{timestamp}{ext}"


@app.get("/", response_class=HTMLResponse)
async def upload_form(request: Request):
    return templates.TemplateResponse("upload.html", {"request": request, "response": None})

@app.post("/upload/", response_class=HTMLResponse)
async def upload_file(request: Request, file: UploadFile = File(...)):
    contents = await file.read()
    sanitized_name = sanitize_filename(file.filename)
    s3.put_object(Bucket=MINIO_BUCKET, Key=sanitized_name, Body=contents)
    response = {"original_filename": file.filename, "stored_as": sanitized_name, "status": "uploaded"}
    return templates.TemplateResponse("upload.html", {"request": request, "response": response})
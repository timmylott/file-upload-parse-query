# 📘 Data Ingestion and Processing Pipeline

This project provides a complete local data ingestion pipeline that automatically uploads files, queues processing jobs, and loads structured data into **Apache Iceberg** tables using **Spark** — all running in **Docker**.

---

## ⚙️ Architecture Overview

The system is composed of the following services:

- **FastAPI App**  
  Provides a REST endpoint to upload files (PDFs, CSVs, etc.).  
  When a file is uploaded:
  - It is stored in **MinIO** (an S3-compatible object store).  
  - A message is added to a **Redis queue** to trigger processing.

- **Redis + RQ (Redis Queue)**  
  Acts as the message broker and job queue.  
  - Stores jobs representing files to process.
  - Enables decoupling between upload and processing.

- **Spark-Iceberg Worker**  
  The same container that runs Spark is used to process queued jobs.
  - A background worker (`start_worker.sh`) continuously listens for jobs in Redis.
  - Each job runs a Spark task that:
    1. Downloads the file from MinIO.
    2. Extracts structured data (e.g., tables from PDFs using Camelot).
    3. Writes the data into an **Iceberg table** stored in MinIO with metadata in PostgreSQL.

- **PostgreSQL (Iceberg Catalog)**  
  Acts as the Iceberg catalog — storing metadata about tables, partitions, and snapshots.

- **MinIO (Object Store)**  
  Stores uploaded files and Iceberg table data.

- **Redis Dashboard**  
  A simple UI to monitor queued, running, and completed jobs.

---

## 🧩 Workflow

1. **User uploads a file** via FastAPI (`POST /upload`).  
2. FastAPI saves the file to **MinIO** and queues a job in **Redis**.  
3. The **Spark worker** running inside the `spark-iceberg` container picks up the job.  
4. Spark reads the file, parses its contents (e.g., with Camelot for PDFs).  
5. Processed data is written to an **Iceberg table** (`default.<table_name>`) in MinIO.  
6. Results can be queried via Spark or Trino.

---

## 🌐 Local Access URLs

| Service              | URL |
|----------------------|-----|
| **FastAPI Upload App** | http://127.0.0.1:8000/docs |
| **MinIO Console**      | http://127.0.0.1:9001 |
| **Redis Dashboard**    | http://127.0.0.1:9181 |

---

## 🐳 Docker Services

- `fastapi` — Upload service  
- `redis` — Message broker  
- `redis-dashboard` — Job monitoring  
- `spark-iceberg` — Spark runtime and worker  
- `postgres` — Iceberg catalog  
- `minio` — Object storage  

---

## 🚀 How to Run

1. **Clone the repository**
   ```bash
   git clone https://github.com/<your-repo>.git
   cd <your-repo>
   ```

2. **Start all services**
   ```bash
   docker compose up -d
   ```

3. **Access the FastAPI interface**
   Open your browser and go to:  
   👉 [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)

4. **Upload a file**
   - Use the `/upload` endpoint in FastAPI to upload a CSV or PDF.
   - The file is stored in MinIO and queued for processing.

5. **Monitor processing**
   - Open [http://127.0.0.1:9181](http://127.0.0.1:9181) to view job status.

6. **Query processed data**
   - Connect to Spark or Trino and run SQL queries against the Iceberg tables stored in MinIO.

---

✅ **End-to-end Result:**  
Uploaded files are automatically ingested, parsed, and stored in an Iceberg data lake, ready for analytics and query engines.
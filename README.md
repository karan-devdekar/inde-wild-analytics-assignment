# Inde_Wild_Analytics
An automated, serverless ELT pipeline that ingests daily sales data from multiple marketplaces (Blinkit, Zepto, Nykaa, and Myntra) into Google BigQuery for centralized analytics.

# Tools and Technology used
Language: Python 3.11+ (Pandas), SQL
Cloud: Google Cloud Platform (GCP)
Compute: Cloud Functions
Storage: Google Cloud Storage
Data Warehouse: BigQuery
Orchestration: Eventarc

# Design Architecture
<img width="1046" height="368" alt="image" src="https://github.com/user-attachments/assets/cd50d9b8-13a9-4da2-8b82-8f2c5a405139" />

# Description
Ingestion: Local CSV files are synced to Google Cloud Storage (GCS) via gsutil rsync.
Trigger: A GCS Eventarc trigger detects new file uploads.
Processing: A Cloud Function (Python,SQL) standardizes the disparate CSV schemas, handles data cleaning (Pandas), and loads data.
Staging: Data is upserted into marketplace-specific staging tables using a MERGE (Upsert) strategy to prevent duplicates.
Transformation: A post-ingestion SQL script aggregates all staging data into a consolidated daily_sales_fact table for BI reporting.

# Technical Approach
The pipeline architecture was designed to be serverless, event-driven, and cost-optimized, leveraging the Google Cloud Platform (GCP) ecosystem.
The core design principle was "Load-then-Transform" (ELT). 
This approach was chosen because:
Simplicity & Speed: Ingesting raw (cleaned) data into BigQuery is extremely fast.
Serverless Cost Efficiency: We only pay for the exact milliseconds the Cloud Function runs and the storage/querying in BigQuery. There are no idle servers or expensive clusters to maintain.
Robust Orchestration: 2nd Gen Cloud Functions provided the ideal compute solution. Their native integration with GCS Eventarc Triggers created a robust, auto-scaling pipeline. We don't need a complex orchestration tool (like Airflow) because the GCS upload itself is the signal to process.

# Design Assumptions
1.Ingestion Cadence (Batch Processing):
Assumption: Data is received as a full batch of sales data once a month (e.g., at the 1st of every month).
Pipeline Behavior: The Python Script to load CSV files to GCS is scheduled (via cron job) to run at the 1st of every month to move the accumulated CSVs into the GCS landing zone. This triggers the ingestion flow.

2.Record Duplication & Re-uploads
Assumption: Sales entries might be repeated across monthly batches, or a file might be re-uploaded due to an earlier error.
Pipeline Behavior: The implementation uses an idempotent MERGE (Upsert) strategy when moving data from a temporary table into the marketplace staging tables (stg_*). A specific composite key (e.g., Date + SKU + City) ensures duplicates are updated, not duplicated.

3.Pre-Staging Normalization
Assumption: The data from different marketplaces is disparate in schema like capitalization, special characters, column naming.
Pipeline Behavior: A global standardization step (converting column names to lowercase, using underscores instead of spaces, removing special characters) is performed in the Cloud Function using Pandas before any data is loaded into the staging tables. This prevents BigQuery BadRequest schema errors.

# Challenges Encountered
Setting Up the GCS-to-Eventarc Trigger Handshake
The major technical challenge was establishing the communication between Google Cloud Storage and the Eventarc trigger.

The Issue: 2nd Gen Cloud Functions introduce an abstraction layer (using Pub/Sub), which requires specific IAM roles to be granted to background service agents—not just my personal account. The pipeline would appear to deploy correctly, but the bucket uploads would silently fail to trigger the function.
The Solution: We had to explicitly grant the roles/pubsub.publisher role to the GCS Service Agent via Cloud Shell. This required identifying the correct system service account and manually applying the binding, highlighting the importance of IAM in serverless architectures.

# Note:
upload_csv.py will upload all the csv files to GCS
gcs_to_bq.py file will load data from GCS to BigQuery

# Artifacts:
# GCS:
<img width="1212" height="364" alt="image" src="https://github.com/user-attachments/assets/d0bc48a8-467d-4ece-b560-76fec55277c3" />

# Cloud Function:
<img width="1055" height="441" alt="image" src="https://github.com/user-attachments/assets/8cfd9c14-d0b5-40c8-91a5-6ae0ddc372aa" />
<img width="1078" height="307" alt="image" src="https://github.com/user-attachments/assets/ebd14b7a-10bd-43ca-b5e2-72e3419bf827" />

# Eventarc Trigger
<img width="1704" height="444" alt="image" src="https://github.com/user-attachments/assets/bfd91cf1-ec39-4b39-89f2-b8cd978635de" />

# BigQuery STG
<img width="1456" height="335" alt="image" src="https://github.com/user-attachments/assets/799b6b76-85de-4c22-b347-6fdc8550e29d" />

# BigQuery Final
<img width="1549" height="752" alt="image" src="https://github.com/user-attachments/assets/d567f7e4-8d27-461b-887c-10701e8632cd" />



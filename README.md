# API -> BigQuery Incremental Pipeline (GCP)

This project ingests data from the GitHub Issues API into Google BigQuery.

## Features
- Token-based auth (GITHUB_TOKEN)
- Incremental loads using watermark (`updated_at`)
- Retries + basic rate limit handling
- Raw append-only table + curated MERGE upsert table
- Local state persistence


## Production on GCP Notebook
This script can be scheduled on GCP using:
- Cloud Scheduler -> Cloud Run (recommended lightweight option)

## Run
```bash
python src/pipeline.py --repo "googleapis/python-bigquery" 
python src/pipeline.py --repo "apache/airflow" --dataset "de_demo"




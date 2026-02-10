# API -> BigQuery Incremental Pipeline (GCP)

This project ingests data from the GitHub Issues API into Google BigQuery.

## Features
- Token-based auth (GITHUB_TOKEN)
- Incremental loads using watermark (`updated_at`)
- Retries + basic rate limit handling
- Raw append-only table + curated MERGE upsert table
- Local state persistence

## Run
```bash
python src/pipeline.py --repo "apache/airflow" --dataset "de_demo" --full_backfill
python src/pipeline.py --repo "apache/airflow" --dataset "de_demo"

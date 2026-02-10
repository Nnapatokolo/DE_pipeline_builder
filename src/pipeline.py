"""
Trustpilot -> BigQuery (Raw + Curated) Pipeline
==============================================

What this script does:
1) Backfill mode:
   - Starts from earliest available Trustpilot data
   - Pulls reviews in chunks of 1000 using cursor pagination
   - Appends everything into a RAW BigQuery table
   - MERGEs from RAW into the final CURATED table (Trustpilot_review)

2) Incremental mode (daily Cloud Scheduler at 08:00):
   - Reads the latest postedAt already present in CURATED table
   - Fetches Trustpilot data posted after that time (still in chunks of 1000 with cursor)
   - Appends new rows to RAW
   - MERGEs into CURATED (idempotent)

BigQuery naming:
- CURATED table:  PROJECT.DATASET.Trustpilot_review
- RAW table:      PROJECT.DATASET.Trustpilot_review_raw

Security:
- Trustpilot API access token is read from the TRUSTPILOT_TOKEN environment variable
- Credentials are never hardcoded in the codebase

Execution examples:
Backfill (historical load):
  python src/pipeline.py \
    --api_base "https://YOUR_TP_BASE" \
    --bq_table "BC.marketing12231233.Trustpilot_review" \
    --full_backfill

- Incremental (daily run):
  Triggered via Cloud Scheduler (daily at 08:00)
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery


# -----------------------------
# Config (simple + readable)
# -----------------------------

PAGE_SIZE = 1000  # This is because for every pull id limited to 1000
BQ_LOCATION = "EU"  # Eco friendly for based on UK standard 


# -----------------------------
# Logging
# -----------------------------

def setup_logging() -> None:
    """
    Logs are essential for production pipelines (debugging, audit, monitoring).
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# -----------------------------
# Helpers
# -----------------------------

def safe_get_env(name: str) -> str:
    """
    Reads a required environment variable.
    We do this so secrets never go into the Git repo.
    """
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val


def parse_bq_table(full_table: str) -> Tuple[str, str, str]:
    """
    BigQuery full table format is:
      BC.marketing12231233.Trustpilot_review
    """
    parts = full_table.split(".")
    if len(parts) != 3:
        raise ValueError('Expected BigQuery table as "BC.marketing12231233.Trustpilot_review'")
    return parts[0], parts[1], parts[2]


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def iso_to_rfc3339_z(ts: str) -> str:
    """
    Normalize ISO timestamps to UTC RFC3339 ("Z") format.
    """
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return ts


# -----------------------------
# HTTP with retries
# -----------------------------

# -----------------------------
# HTTP requests with retry logic
# -----------------------------

def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    timeout_seconds: int = 30,
) -> requests.Response:
    """
    Execute an HTTP request with retry handling for transient failures.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(
                method,
                url,
                headers=headers,
                params=params,
                timeout=timeout_seconds,
            )

            # Rate limiting
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                wait = int(retry_after) if retry_after else min(
                    60, (2 ** attempt) + random.randint(0, 3)
                )
                logging.warning(f"Rate limited (429). Retrying in {wait}s.")
                time.sleep(wait)
                continue

            # Transient server errors
            if 500 <= resp.status_code < 600:
                wait = min(60, (2 ** attempt) + random.randint(0, 3))
                logging.warning(f"Server error {resp.status_code}. Retrying in {wait}s.")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            return resp

        except Exception as e:
            last_exc = e
            wait = min(60, (2 ** attempt) + random.randint(0, 3))
            logging.warning(
                f"Request attempt {attempt}/{max_retries} failed: {e}. Retrying in {wait}s."
            )
            time.sleep(wait)

    raise RuntimeError(
        f"HTTP request failed after {max_retries} retries. Last error: {last_exc}"
    )


# -----------------------------
# Trustpilot Record Model
# -----------------------------

@dataclass
class TPReview:
    """
    Normalized Trustpilot review record used for BigQuery ingestion.
    """
    review_id: str
    date: str
    tp_user_id: str
    bc_number: str
    review: str
    rating: int
    raw_payload: str
    ingested_at: str


def normalize_tp_item(item: Dict[str, Any]) -> TPReview:
    """
    Normalize a raw Trustpilot API record into the internal TPReview schema.
    """
    review_id = str(item.get("id", ""))
    posted_at = iso_to_rfc3339_z(str(item.get("postedAt", "")))
    tp_user_id = str(item.get("userId", ""))
    bc_number = str(item.get("orderNumber", ""))  # BC Number = order number
    review_text = str(item.get("review", ""))
    rating_val = item.get("rating", 0)

    try:
        rating_int = int(rating_val) if rating_val is not None else 0
    except Exception:
        rating_int = 0

    return TPReview(
        review_id=review_id,
        date=posted_at,
        tp_user_id=tp_user_id,
        bc_number=bc_number,
        review=review_text,
        rating=rating_int,
        raw_payload=json.dumps(item, ensure_ascii=False),
        ingested_at=utc_now_iso(),
    )


# -----------------------------
# Trustpilot Extract: cursor pagination
# This function handles the Truspilot extraction using cursor-based pagination
# -----------------------------

def fetch_all_tp_reviews(
    api_base: str,
    token: str,
    posted_after: Optional[str],
) -> List[TPReview]:
    """
    Fetch Trustpilot reviews using cursor-based pagination.

    - Supports full backfill when posted_after is None
    - Supports incremental loads when posted_after is provided
    - Continues paging until the API indicates no more data
    """
    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "tp-to-bigquery-pipeline",
    }

    # Trustpilot reviews endpoint (path may vary by account/environment)
    url = f"{api_base}/reviews"

    cursor: Optional[str] = None
    all_rows: List[TPReview] = []

    while True:
        params: Dict[str, Any] = {"pageSize": PAGE_SIZE}

        # Continue from the previous position if a cursor is present
        if cursor:
            params["cursor"] = cursor

        # Apply incremental filter when available
        if posted_after:
            params["postedAfter"] = posted_after

        resp = request_with_retries(session, "GET", url, headers=headers, params=params)
        payload = resp.json()

        items = payload.get("items", [])
        if not items:
            break

        # Normalize raw API records into the internal TPReview model
        batch = [normalize_tp_item(x) for x in items if isinstance(x, dict)]
        all_rows.extend(batch)

        next_cursor = payload.get("nextCursor")
        logging.info(
            f"Fetched {len(batch)} reviews | total={len(all_rows)} | "
            f"next_cursor={'yes' if next_cursor else 'no'}"
        )

        if not next_cursor:
            break
        cursor = next_cursor

    return all_rows


# -----------------------------
# BigQuery: dataset + tables (partitioning best practice)
#This part has to do with linking both table and ensuring optimisation
# -----------------------------

def ensure_dataset_and_tables(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    curated_table: str,
) -> Tuple[str, str]:
    """
    Ensure the BigQuery dataset and required tables exist.

    Creates:
    - RAW table (append-only, partitioned by ingestion time)
    - CURATED table (upsert target, partitioned by business date)
    """
    dataset_ref = bigquery.DatasetReference(project_id, dataset_id)

    # Create dataset if it does not exist
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = BQ_LOCATION
        client.create_dataset(ds)
        logging.info(f"Created dataset {project_id}:{dataset_id} ({BQ_LOCATION})")

    raw_table = f"{curated_table}_raw"

    raw_ref = dataset_ref.table(raw_table)
    curated_ref = dataset_ref.table(curated_table)

    schema = [
        bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("tp_user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bc_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("review", "STRING"),
        bigquery.SchemaField("rating", "INT64"),
        bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("raw_payload", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    # RAW table: append-only
    try:
        client.get_table(raw_ref)
    except Exception:
        t = bigquery.Table(raw_ref, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        )
        client.create_table(t)
        logging.info(f"Created RAW table {project_id}.{dataset_id}.{raw_table}")

    # CURATED table: merge target
    try:
        client.get_table(curated_ref)
    except Exception:
        t = bigquery.Table(curated_ref, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        t.clustering_fields = ["bc_number"]
        client.create_table(t)
        logging.info(f"Created CURATED table {project_id}.{dataset_id}.{curated_table}")

    return raw_table, curated_table


def load_raw_rows(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table: str,
    rows: List[TPReview],
) -> None:
    """
    Append records to the RAW BigQuery table.
    """
    if not rows:
        logging.info("No new rows to load into RAW.")
        return

    table_id = f"{project_id}.{dataset_id}.{raw_table}"
    payload: List[Dict[str, Any]] = []

    for r in rows:
        payload.append({
            "date": r.date,
            "tp_user_id": r.tp_user_id,
            "bc_number": r.bc_number,
            "review": r.review,
            "rating": r.rating,
            "review_id": r.review_id,
            "raw_payload": r.raw_payload,
            "ingested_at": r.ingested_at,
        })

    errors = client.insert_rows_json(table_id, payload)
    if errors:
        raise RuntimeError(f"BigQuery insert_rows_json errors: {errors}")

    logging.info(f"Inserted {len(payload)} rows into RAW table {table_id}")


def merge_raw_to_curated(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    raw_table: str,
    curated_table: str,
) -> None:
    """
    Merge RAW data into CURATED using an idempotent upsert strategy.
    """
    raw = f"`{project_id}.{dataset_id}.{raw_table}`"
    curated = f"`{project_id}.{dataset_id}.{curated_table}`"

    sql = f"""
    MERGE {curated} T
    USING (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          date, tp_user_id, bc_number, review, rating, review_id, raw_payload, ingested_at,
          ROW_NUMBER() OVER (
            PARTITION BY review_id, bc_number
            ORDER BY ingested_at DESC
          ) AS rn
        FROM {raw}
      )
      WHERE rn = 1
    ) S
    ON T.review_id = S.review_id AND T.bc_number = S.bc_number
    WHEN MATCHED THEN
      UPDATE SET
        date = S.date,
        tp_user_id = S.tp_user_id,
        review = S.review,
        rating = S.rating,
        raw_payload = S.raw_payload,
        ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN
      INSERT (date, tp_user_id, bc_number, review, rating, review_id, raw_payload, ingested_at)
      VALUES (S.date, S.tp_user_id, S.bc_number, S.review, S.rating, S.review_id, S.raw_payload, S.ingested_at)
    """

    job = client.query(sql)
    job.result()
    logging.info("MERGE RAW -> CURATED completed.")

def get_max_posted_at_from_curated(
    client: bigquery.Client,
    project_id: str,
    dataset_id: str,
    curated_table: str,
) -> Optional[str]:
    """
    Retrieve the latest postedAt value from the CURATED table
    to use as the incremental watermark.
    """
    sql = f"""
    SELECT MAX(date) AS max_date
    FROM `{project_id}.{dataset_id}.{curated_table}`
    """
    results = list(client.query(sql).result())
    if not results or results[0]["max_date"] is None:
        return None

    max_dt = results[0]["max_date"]
    if isinstance(max_dt, datetime):
        return max_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return str(max_dt)

# -----------------------------
# Main orchestration
#This is where the magic happens 
# -----------------------------

def main() -> int:
    """
    Orchestrates the end-to-end Trustpilot to BigQuery pipeline.
    """
    setup_logging()

    parser = argparse.ArgumentParser()

    # Trustpilot API base URL (environment-specific)
    parser.add_argument("--api_base", required=True)

    # BigQuery target table in the form PROJECT.DATASET.Trustpilot_review
    parser.add_argument("--bq_table", required=True)

    # When set, ignore existing data and pull all available history
    parser.add_argument("--full_backfill", action="store_true")

    args = parser.parse_args()

    # Read Trustpilot API token from environment
    tp_token = safe_get_env("TRUSTPILOT_TOKEN")

    # Parse BigQuery identifiers from full table reference
    project_id, dataset_id, curated_table = parse_bq_table(args.bq_table)

    # Initialize BigQuery client (uses Application Default Credentials)
    bq = bigquery.Client(project=project_id)

    # Ensure dataset and tables exist
    raw_table, curated_table = ensure_dataset_and_tables(
        client=bq,
        project_id=project_id,
        dataset_id=dataset_id,
        curated_table=curated_table,
    )

    # Determine whether to run a full backfill or an incremental load
    if args.full_backfill:
        posted_after = None
        logging.info("Running full backfill.")
    else:
        posted_after = get_max_posted_at_from_curated(
            bq, project_id, dataset_id, curated_table
        )
        logging.info(
            f"Running incremental load with watermark: {posted_after}"
        )

    # Extract reviews from Trustpilot using cursor-based pagination
    rows = fetch_all_tp_reviews(
        api_base=args.api_base,
        token=tp_token,
        posted_after=posted_after,
    )

    # Load data into RAW table and merge into CURATED table
    if rows:
        load_raw_rows(bq, project_id, dataset_id, raw_table, rows)
        merge_raw_to_curated(bq, project_id, dataset_id, raw_table, curated_table)
    else:
        logging.info("No new data returned from Trustpilot.")

    logging.info("Pipeline execution completed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


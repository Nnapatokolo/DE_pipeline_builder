"""
Trustpilot -> BigQuery Incremental Pipeline (cursor-based)

Ingests Trustpilot reviews into BigQuery.
Features:
- Token-based auth (TRUSTPILOT_TOKEN)
- Cursor-based pagination (chunked pulls)
- Incremental loads using watermark (postedAt)
- Retries with exponential backoff
- Raw table (append) + Curated table (MERGE upsert)
- Local state persistence for resumable runs

BigQuery curated schema (matches your target):
- date (from postedAt)
- tp_user_id
- bc_number (order number)
- review
- rating
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
from typing import Any, Dict, List, Optional

import requests
from google.cloud import bigquery


# -----------------------------
# Config / Constants
# -----------------------------

STATE_DIR = ".state"
STATE_FILE = "trustpilot_reviews_state.json"

DEFAULT_PAGE_SIZE = 1000  # Trustpilot often limits batch size (adjust as needed)


# -----------------------------
# Logging
# -----------------------------

def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


# -----------------------------
# Helpers
# -----------------------------

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def load_state() -> Dict[str, Any]:
    """
    Stores incremental checkpoints:
      {
        "last_posted_at": "2024-01-01T00:00:00Z",
        "last_cursor": null
      }
    """
    ensure_dir(STATE_DIR)
    path = os.path.join(STATE_DIR, STATE_FILE)
    if not os.path.exists(path):
        return {"last_posted_at": None, "last_cursor": None}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state: Dict[str, Any]) -> None:
    ensure_dir(STATE_DIR)
    path = os.path.join(STATE_DIR, STATE_FILE)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def safe_get_env(name: str) -> str:
    val = os.getenv(name)
    if not val:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return val

def request_with_retries(
    session: requests.Session,
    method: str,
    url: str,
    headers: Dict[str, str],
    params: Optional[Dict[str, Any]] = None,
    json_body: Optional[Dict[str, Any]] = None,
    max_retries: int = 5,
    timeout_seconds: int = 30,
) -> requests.Response:
    """
    Retries on transient failures and rate limiting.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(
                method,
                url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=timeout_seconds,
            )

            if resp.status_code in (429, 403):
                retry_after = resp.headers.get("Retry-After")
                wait_seconds = int(retry_after) if retry_after else min(60, (2 ** attempt) + random.randint(0, 3))
                logging.warning(f"Throttled (status {resp.status_code}). Waiting {wait_seconds}s then retrying.")
                time.sleep(wait_seconds)
                continue

            if 500 <= resp.status_code < 600:
                wait_seconds = min(60, (2 ** attempt) + random.randint(0, 3))
                logging.warning(f"Server error {resp.status_code}. Waiting {wait_seconds}s then retrying.")
                time.sleep(wait_seconds)
                continue

            resp.raise_for_status()
            return resp

        except Exception as e:
            last_exc = e
            wait_seconds = min(60, (2 ** attempt) + random.randint(0, 3))
            logging.warning(f"Request failed (attempt {attempt}/{max_retries}): {e}. Waiting {wait_seconds}s.")
            time.sleep(wait_seconds)

    raise RuntimeError(f"Failed request after {max_retries} retries. Last error: {last_exc}")


# -----------------------------
# Trustpilot Extract (cursor-based)
# -----------------------------

@dataclass
class TrustpilotReviewRecord:
    # Target curated schema
    date: str              # postedAt
    tp_user_id: str
    bc_number: str         # order number
    review: str
    rating: int

    # Optional robustness fields (recommended)
    raw_payload: str
    ingested_at: str

def _get(d: Dict[str, Any], path: List[str], default: Any = None) -> Any:
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur

def normalize_review_item(item: Dict[str, Any]) -> TrustpilotReviewRecord:
    """
    This expects Trustpilot-like keys. Adjust paths to match your actual API response.
    You told me your source has:
      createdAt, orderNumber, userId, review, postedAt, rating
    """
    posted_at = item.get("postedAt") or item.get("posted_at") or item.get("date") or ""
    tp_user_id = str(item.get("userId") or item.get("user_id") or "")
    bc_number = str(item.get("orderNumber") or item.get("order_number") or "")
    review_text = str(item.get("review") or item.get("reviewText") or "")
    rating = item.get("rating")
    try:
        rating_int = int(rating) if rating is not None else 0
    except Exception:
        rating_int = 0

    return TrustpilotReviewRecord(
        date=posted_at,
        tp_user_id=tp_user_id,
        bc_number=bc_number,
        review=review_text,
        rating=rating_int,
        raw_payload=json.dumps(item, ensure_ascii=False),
        ingested_at=utc_now_iso(),
    )

def fetch_trustpilot_reviews(
    api_base: str,
    token: str,
    initial_cursor: Optional[str],
    posted_after: Optional[str],
    page_size: int = DEFAULT_PAGE_SIZE,
) -> List[TrustpilotReviewRecord]:
    """
    Cursor pagination loop.
    Assumes the API returns:
      {
        "items": [...],
        "nextCursor": "...." (or null)
      }

    Adjust endpoint/params to match your Trustpilot setup.
    """
    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "trustpilot-to-bigquery-pipeline",
    }

    # You must set the correct Trustpilot endpoint for your environment.
    # Example placeholder endpoint:
    url = f"{api_base}/reviews"

    cursor = initial_cursor
    out: List[TrustpilotReviewRecord] = []

    while True:
        params: Dict[str, Any] = {"pageSize": page_size}
        if cursor:
            params["cursor"] = cursor
        if posted_after:
            # Use whatever filter your TP endpoint supports
            params["postedAfter"] = posted_after

        resp = request_with_retries(session, "GET", url, headers=headers, params=params)
        payload = resp.json()

        items = payload.get("items") or payload.get("data") or []
        if not items:
            break

        batch = [normalize_review_item(x) for x in items if isinstance(x, dict)]
        out.extend(batch)

        next_cursor = payload.get("nextCursor") or payload.get("next_cursor")
        logging.info(f"Fetched {len(batch)} reviews (cursor={'set' if cursor else 'none'}). next_cursor={'yes' if next_cursor else 'no'}")

        if not next_cursor:
            break
        cursor = next_cursor

    return out

def compute_new_posted_watermark(records: List[TrustpilotReviewRecord], previous: Optional[str]) -> Optional[str]:
    """
    Returns max postedAt (date field) for incremental loads.
    """
    if not records:
        return previous

    max_dt = None
    for r in records:
        # Accept either "Z" or "+00:00" formats
        try:
            dt = datetime.fromisoformat(r.date.replace("Z", "+00:00"))
        except Exception:
            continue
        if max_dt is None or dt > max_dt:
            max_dt = dt

    if max_dt is None:
        return previous

    return max_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# -----------------------------
# BigQuery Load + Merge
# -----------------------------

def ensure_tables(
    client: bigquery.Client,
    dataset_id: str,
    raw_table_id: str,
    curated_table_id: str,
) -> None:
    dataset_ref = bigquery.DatasetReference(client.project, dataset_id)
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = "EU"  # change if needed
        client.create_dataset(ds)
        logging.info(f"Created dataset {client.project}:{dataset_id}")

    raw_ref = dataset_ref.table(raw_table_id)
    curated_ref = dataset_ref.table(curated_table_id)

    # Raw/Curated schema (your target fields + robustness fields)
    schema = [
        bigquery.SchemaField("date", "TIMESTAMP", mode="REQUIRED"),     # postedAt
        bigquery.SchemaField("tp_user_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("bc_number", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("review", "STRING"),
        bigquery.SchemaField("rating", "INT64"),
        bigquery.SchemaField("raw_payload", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    # Raw table: append-only, partitioned by ingested_at
    try:
        client.get_table(raw_ref)
    except Exception:
        t = bigquery.Table(raw_ref, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        )
        client.create_table(t)
        logging.info(f"Created raw table {client.project}:{dataset_id}.{raw_table_id}")

    # Curated table: partitioned by date (postedAt)
    try:
        client.get_table(curated_ref)
    except Exception:
        t = bigquery.Table(curated_ref, schema=schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="date",
        )
        client.create_table(t)
        logging.info(f"Created curated table {client.project}:{dataset_id}.{curated_table_id}")

def load_raw(
    client: bigquery.Client,
    dataset_id: str,
    raw_table_id: str,
    records: List[TrustpilotReviewRecord],
) -> None:
    if not records:
        logging.info("No records to load into raw table.")
        return

    table = f"{client.project}.{dataset_id}.{raw_table_id}"

    rows: List[Dict[str, Any]] = []
    for r in records:
        rows.append(
            {
                "date": r.date,
                "tp_user_id": r.tp_user_id,
                "bc_number": r.bc_number,
                "review": r.review,
                "rating": r.rating,
                "raw_payload": r.raw_payload,
                "ingested_at": r.ingested_at,
            }
        )

    errors = client.insert_rows_json(table, rows)
    if errors:
        raise RuntimeError(f"BigQuery insert_rows_json failed: {errors}")

    logging.info(f"Loaded {len(rows)} rows into raw table {table}")

def merge_to_curated(
    client: bigquery.Client,
    dataset_id: str,
    raw_table_id: str,
    curated_table_id: str,
) -> None:
    """
    Upsert into curated.
    Since Trustpilot reviews may not have a single stable unique ID in your provided schema,
    we use a composite key: (tp_user_id, bc_number, date).
    If you DO have a stable review_id, use it instead (best practice).
    """
    raw = f"`{client.project}.{dataset_id}.{raw_table_id}`"
    curated = f"`{client.project}.{dataset_id}.{curated_table_id}`"

    sql = f"""
    MERGE {curated} T
    USING (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          date, tp_user_id, bc_number, review, rating, raw_payload, ingested_at,
          ROW_NUMBER() OVER (
            PARTITION BY tp_user_id, bc_number, date
            ORDER BY ingested_at DESC
          ) AS rn
        FROM {raw}
      )
      WHERE rn = 1
    ) S
    ON T.tp_user_id = S.tp_user_id
       AND T.bc_number = S.bc_number
       AND T.date = S.date
    WHEN MATCHED THEN
      UPDATE SET
        review = S.review,
        rating = S.rating,
        raw_payload = S.raw_payload,
        ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN
      INSERT (date, tp_user_id, bc_number, review, rating, raw_payload, ingested_at)
      VALUES (S.date, S.tp_user_id, S.bc_number, S.review, S.rating, S.raw_payload, S.ingested_at)
    """

    job = client.query(sql)
    job.result()
    logging.info("MERGE into curated table completed.")


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--api_base", required=True, help='Trustpilot API base URL e.g. "https://api.trustpilot.com/v1"')
    parser.add_argument("--dataset", required=True, help="BigQuery dataset id (created if missing)")
    parser.add_argument("--raw_table", default="tp_reviews_raw", help="Raw table name")
    parser.add_argument("--cur_table", default="tp_reviews_curated", help="Curated table name")
    parser.add_argument("--full_backfill", action="store_true", help="Ignore watermark/cursor and backfill everything")
    args = parser.parse_args()

    token = safe_get_env("TRUSTPILOT_TOKEN")

    # Uses Application Default Credentials (ADC) for GCP:
    # e.g., `gcloud auth application-default login`
    bq_client = bigquery.Client()

    ensure_tables(bq_client, args.dataset, args.raw_table, args.cur_table)

    state = load_state()
    posted_after = None if args.full_backfill else state.get("last_posted_at")
    cursor = None if args.full_backfill else state.get("last_cursor")

    logging.info(f"Starting Trustpilot ingestion (posted_after={posted_after}, cursor={'set' if cursor else 'none'})")
    records = fetch_trustpilot_reviews(
        api_base=args.api_base,
        token=token,
        initial_cursor=cursor,
        posted_after=posted_after,
    )

    if records:
        load_raw(bq_client, args.dataset, args.raw_table, records)
        merge_to_curated(bq_client, args.dataset, args.raw_table, args.cur_table)

    # Update watermark for next run
    state["last_posted_at"] = compute_new_posted_watermark(records, posted_after)
    # Cursor is usually only useful within a single traversal; for daily increments watermark is enough.
    # If your TP API requires cursor continuation across runs, store the final cursor instead.
    state["last_cursor"] = None
    save_state(state)

    logging.info(f"Done. Records fetched: {len(records)}. New posted_at watermark: {state['last_posted_at']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

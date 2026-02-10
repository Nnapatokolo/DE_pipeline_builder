"""
API -> BigQuery Incremental Pipeline (Trustpilot-style)

Ingests GitHub Issues for a repo into BigQuery.
Features:
- Token-based auth (GITHUB_TOKEN)
- Incremental loads using watermark (updated_at)
- Retries with exponential backoff
- Raw table (append) + Curated table (MERGE upsert)
- Basic logging and safe state persistence

Usage:
  python pipeline.py --repo "apache/airflow" --dataset "de_demo" --raw_table "issues_raw" --cur_table "issues_curated"
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from google.cloud import bigquery


# -----------------------------
# Config / Constants
# -----------------------------

GITHUB_API_BASE = "https://api.github.com"
DEFAULT_PER_PAGE = 100  # GitHub max is 100
STATE_DIR = ".state"
STATE_FILE_TEMPLATE = "github_issues_{owner}_{repo}.json"


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

def load_state(owner: str, repo: str) -> Dict[str, Any]:
    """
    State stores the watermark for incremental loads:
      {"last_updated_at": "2024-01-01T00:00:00Z"}
    """
    ensure_dir(STATE_DIR)
    path = os.path.join(STATE_DIR, STATE_FILE_TEMPLATE.format(owner=owner, repo=repo))
    if not os.path.exists(path):
        return {"last_updated_at": None}

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(owner: str, repo: str, state: Dict[str, Any]) -> None:
    ensure_dir(STATE_DIR)
    path = os.path.join(STATE_DIR, STATE_FILE_TEMPLATE.format(owner=owner, repo=repo))
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)

def parse_repo(full_repo: str) -> Tuple[str, str]:
    if "/" not in full_repo:
        raise ValueError('Repo must be in "owner/repo" format.')
    owner, repo = full_repo.split("/", 1)
    return owner.strip(), repo.strip()

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
    max_retries: int = 5,
    timeout_seconds: int = 30,
) -> requests.Response:
    """
    Retries on transient failures and rate limiting.
    Handles GitHub rate limit by respecting Retry-After when present, else backoff.
    """
    last_exc: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            resp = session.request(method, url, headers=headers, params=params, timeout=timeout_seconds)

            # Rate limit / throttling
            if resp.status_code in (429, 403):
                retry_after = resp.headers.get("Retry-After")
                reset = resp.headers.get("X-RateLimit-Reset")
                remaining = resp.headers.get("X-RateLimit-Remaining")

                # If explicitly rate limited (remaining == 0), wait until reset
                if remaining == "0" and reset:
                    wait_seconds = max(1, int(reset) - int(time.time()))
                    logging.warning(f"Rate limited. Waiting {wait_seconds}s until reset.")
                    time.sleep(wait_seconds)
                    continue

                if retry_after:
                    wait_seconds = int(retry_after)
                else:
                    wait_seconds = min(60, (2 ** attempt) + random.randint(0, 3))
                logging.warning(f"Throttled (status {resp.status_code}). Waiting {wait_seconds}s then retrying.")
                time.sleep(wait_seconds)
                continue

            # Retry on server errors
            if 500 <= resp.status_code < 600:
                wait_seconds = min(60, (2 ** attempt) + random.randint(0, 3))
                logging.warning(f"Server error {resp.status_code}. Waiting {wait_seconds}s then retrying.")
                time.sleep(wait_seconds)
                continue

            # For other errors, raise
            resp.raise_for_status()
            return resp

        except Exception as e:
            last_exc = e
            wait_seconds = min(60, (2 ** attempt) + random.randint(0, 3))
            logging.warning(f"Request failed (attempt {attempt}/{max_retries}): {e}. Waiting {wait_seconds}s.")
            time.sleep(wait_seconds)

    raise RuntimeError(f"Failed request after {max_retries} retries. Last error: {last_exc}")


# -----------------------------
# GitHub Extract
# -----------------------------

@dataclass
class IssueRecord:
    issue_id: int
    number: int
    title: str
    state: str
    created_at: str
    updated_at: str
    closed_at: Optional[str]
    user_login: Optional[str]
    labels: List[str]
    url: str
    raw_payload: str
    ingested_at: str

def flatten_issue(issue: Dict[str, Any]) -> IssueRecord:
    labels = [l.get("name") for l in issue.get("labels", []) if isinstance(l, dict) and l.get("name")]
    user = issue.get("user") or {}
    return IssueRecord(
        issue_id=int(issue["id"]),
        number=int(issue.get("number") or 0),
        title=str(issue.get("title") or ""),
        state=str(issue.get("state") or ""),
        created_at=str(issue.get("created_at") or ""),
        updated_at=str(issue.get("updated_at") or ""),
        closed_at=issue.get("closed_at"),
        user_login=user.get("login"),
        labels=labels,
        url=str(issue.get("html_url") or issue.get("url") or ""),
        raw_payload=json.dumps(issue, ensure_ascii=False),
        ingested_at=utc_now_iso(),
    )

def fetch_github_issues_incremental(
    owner: str,
    repo: str,
    token: str,
    since_iso: Optional[str],
) -> List[IssueRecord]:
    """
    Incremental ingestion using the 'since' parameter.
    It's not a literal cursor token, but acts as a watermark cursor (updated_at).
    This mirrors cursor-based incremental ingestion patterns used by many APIs.
    """
    session = requests.Session()
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "User-Agent": "api-to-bigquery-pipeline",
    }

    url = f"{GITHUB_API_BASE}/repos/{owner}/{repo}/issues"

    page = 1
    all_records: List[IssueRecord] = []

    while True:
        params: Dict[str, Any] = {
            "state": "all",
            "per_page": DEFAULT_PER_PAGE,
            "page": page,
            "sort": "updated",
            "direction": "asc",
        }
        if since_iso:
            params["since"] = since_iso

        resp = request_with_retries(session, "GET", url, headers=headers, params=params)
        data = resp.json()

        # GitHub returns PRs in issues endpoint; filter them out
        issues_only = [x for x in data if isinstance(x, dict) and "pull_request" not in x]

        if not issues_only:
            break

        batch = [flatten_issue(x) for x in issues_only]
        all_records.extend(batch)

        logging.info(f"Fetched page {page}, records {len(batch)} (since={since_iso})")

        # If fewer than per_page, it's the last page
        if len(data) < DEFAULT_PER_PAGE:
            break

        page += 1

    return all_records


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

    raw_schema = [
        bigquery.SchemaField("issue_id", "INT64", mode="REQUIRED"),
        bigquery.SchemaField("number", "INT64"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("state", "STRING"),
        bigquery.SchemaField("created_at", "TIMESTAMP"),
        bigquery.SchemaField("updated_at", "TIMESTAMP"),
        bigquery.SchemaField("closed_at", "TIMESTAMP"),
        bigquery.SchemaField("user_login", "STRING"),
        bigquery.SchemaField("labels", "STRING", mode="REPEATED"),
        bigquery.SchemaField("url", "STRING"),
        bigquery.SchemaField("raw_payload", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
    ]

    # Raw table: append-only
    try:
        client.get_table(raw_ref)
    except Exception:
        t = bigquery.Table(raw_ref, schema=raw_schema)
        # Partition raw table by ingestion time for query efficiency
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="ingested_at",
        )
        client.create_table(t)
        logging.info(f"Created raw table {client.project}:{dataset_id}.{raw_table_id}")

    # Curated table: upsert target
    try:
        client.get_table(curated_ref)
    except Exception:
        t = bigquery.Table(curated_ref, schema=raw_schema)
        t.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="updated_at",
        )
        client.create_table(t)
        logging.info(f"Created curated table {client.project}:{dataset_id}.{curated_table_id}")

def load_raw(
    client: bigquery.Client,
    dataset_id: str,
    raw_table_id: str,
    records: List[IssueRecord],
) -> None:
    if not records:
        logging.info("No records to load into raw table.")
        return

    table = f"{client.project}.{dataset_id}.{raw_table_id}"

    # Convert to JSON rows
    rows: List[Dict[str, Any]] = []
    for r in records:
        rows.append(
            {
                "issue_id": r.issue_id,
                "number": r.number,
                "title": r.title,
                "state": r.state,
                "created_at": r.created_at,
                "updated_at": r.updated_at,
                "closed_at": r.closed_at,
                "user_login": r.user_login,
                "labels": r.labels,
                "url": r.url,
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
    Upsert from raw into curated by issue_id, keeping the latest updated_at per issue.
    """
    raw = f"`{client.project}.{dataset_id}.{raw_table_id}`"
    curated = f"`{client.project}.{dataset_id}.{curated_table_id}`"

    sql = f"""
    MERGE {curated} T
    USING (
      SELECT * EXCEPT(rn)
      FROM (
        SELECT
          issue_id, number, title, state, created_at, updated_at, closed_at,
          user_login, labels, url, raw_payload, ingested_at,
          ROW_NUMBER() OVER (PARTITION BY issue_id ORDER BY updated_at DESC, ingested_at DESC) AS rn
        FROM {raw}
      )
      WHERE rn = 1
    ) S
    ON T.issue_id = S.issue_id
    WHEN MATCHED AND S.updated_at >= T.updated_at THEN
      UPDATE SET
        number = S.number,
        title = S.title,
        state = S.state,
        created_at = S.created_at,
        updated_at = S.updated_at,
        closed_at = S.closed_at,
        user_login = S.user_login,
        labels = S.labels,
        url = S.url,
        raw_payload = S.raw_payload,
        ingested_at = S.ingested_at
    WHEN NOT MATCHED THEN
      INSERT (issue_id, number, title, state, created_at, updated_at, closed_at, user_login, labels, url, raw_payload, ingested_at)
      VALUES (S.issue_id, S.number, S.title, S.state, S.created_at, S.updated_at, S.closed_at, S.user_login, S.labels, S.url, S.raw_payload, S.ingested_at)
    """

    job = client.query(sql)
    job.result()
    logging.info("MERGE into curated table completed.")

def compute_new_watermark(records: List[IssueRecord], previous: Optional[str]) -> Optional[str]:
    """
    Returns the max updated_at among records, as ISO string, to use for the next run.
    """
    if not records:
        return previous

    # updated_at is ISO timestamp string; safe to compare as datetime
    max_dt = None
    for r in records:
        try:
            dt = datetime.fromisoformat(r.updated_at.replace("Z", "+00:00"))
        except Exception:
            continue
        if max_dt is None or dt > max_dt:
            max_dt = dt

    if max_dt is None:
        return previous

    # GitHub expects RFC3339; using ISO with Z works
    return max_dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# -----------------------------
# Main
# -----------------------------

def main() -> int:
    setup_logging()

    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, help='GitHub repo in "owner/repo" format e.g. "apache/airflow"')
    parser.add_argument("--dataset", required=True, help="BigQuery dataset id (will be created if missing)")
    parser.add_argument("--raw_table", default="issues_raw", help="Raw table name")
    parser.add_argument("--cur_table", default="issues_curated", help="Curated table name")
    parser.add_argument("--full_backfill", action="store_true", help="Ignore watermark and backfill everything")
    args = parser.parse_args()

    owner, repo = parse_repo(args.repo)

    token = safe_get_env("GITHUB_TOKEN")
    # Uses Application Default Credentials (ADC) for GCP:
    # e.g., `gcloud auth application-default login`
    bq_client = bigquery.Client()

    ensure_tables(bq_client, args.dataset, args.raw_table, args.cur_table)

    state = load_state(owner, repo)
    since_iso = None if args.full_backfill else state.get("last_updated_at")

    logging.info(f"Starting ingestion for {owner}/{repo} (since={since_iso})")
    records = fetch_github_issues_incremental(owner, repo, token, since_iso)

    if records:
        load_raw(bq_client, args.dataset, args.raw_table, records)
        merge_to_curated(bq_client, args.dataset, args.raw_table, args.cur_table)

    new_wm = compute_new_watermark(records, since_iso)
    state["last_updated_at"] = new_wm
    save_state(owner, repo, state)

    logging.info(f"Done. Records fetched: {len(records)}. New watermark: {new_wm}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

# ─────────────────────────────────────────────
# Crypto Data Ingestion Script
# Fetches live prices from CoinGecko API
# Saves raw JSON to GCS + loads to BigQuery
# ─────────────────────────────────────────────

import requests
import json
import os
import logging
from datetime import datetime, timezone
from google.cloud import storage, bigquery

# ── Config ──────────────────────────────────
PROJECT_ID   = "crypto-pipeline-498217"   # your project id
DATASET_ID   = "crypto_raw"
TABLE_ID     = "raw_prices"
BUCKET_NAME  = "crypto-pipeline-raw-data-kracshan"  # we create this below
COINS        = "bitcoin,ethereum,binancecoin,solana"

# ── Logging ──────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

# ── 1. Fetch from CoinGecko API ──────────────
def fetch_crypto_data():
    log.info("Fetching data from CoinGecko...")
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": "usd",
        "ids": COINS,
        "order": "market_cap_desc",
        "sparkline": "false"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    data = response.json()
    log.info(f"Fetched {len(data)} coins successfully")
    return data

# ── 2. Save raw JSON to GCS bucket ───────────
def save_to_gcs(data):
    log.info("Saving raw JSON to GCS...")
    client  = storage.Client()
    bucket  = client.bucket(BUCKET_NAME)
    ts      = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    blob    = bucket.blob(f"raw/crypto_{ts}.json")
    blob.upload_from_string(
        json.dumps(data, indent=2),
        content_type="application/json"
    )
    log.info(f"Saved to GCS: raw/crypto_{ts}.json")

# ── 3. Load rows into BigQuery ────────────────
def load_to_bigquery(data):
    log.info("Loading data into BigQuery...")
    client   = bigquery.Client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    rows = []
    for coin in data:
        rows.append({
            "coin_id"        : coin["id"],
            "symbol"         : coin["symbol"].upper(),
            "name"           : coin["name"],
            "price_usd"      : coin["current_price"],
            "market_cap"     : coin["market_cap"],
            "volume_24h"     : coin["total_volume"],
            "price_change_24h": coin["price_change_percentage_24h"],
            "fetched_at"     : datetime.now(timezone.utc).isoformat()
        })

    errors = client.insert_rows_json(table_ref, rows)
    if errors:
        log.error(f"BigQuery insert errors: {errors}")
        raise ValueError("Failed to insert rows into BigQuery")
    log.info(f"Loaded {len(rows)} rows into BigQuery")

# ── Main ──────────────────────────────────────
if __name__ == "__main__":
    data = fetch_crypto_data()
    save_to_gcs(data)
    load_to_bigquery(data)
    log.info("Pipeline run complete ✓")
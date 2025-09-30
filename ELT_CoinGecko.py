from __future__ import annotations
import os, json, uuid
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests
import psycopg2
from psycopg2.extras import execute_values

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

PG_DSN = {
    "host": "coingecko-prefect.cwnswisgoomv.us-east-1.rds.amazonaws.com",
    "port": 5432,
    "dbname": "postgres",
    "user": "postgres",
    "password": "-----------",
}

COINGECKO_URL = os.getenv(
    "COINGECKO_URL",
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=100&page=1"
    "&price_change_percentage=1h,24h,7d"
)

def get_conn():
    return psycopg2.connect(**PG_DSN)

@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=None)
def extract_raw(url: str = COINGECKO_URL) -> List[Dict[str, Any]]:
    log = get_run_logger()
    log.info(f"Llamando a: {url}")
    r = requests.get(url, timeout=30)
    r.raise_for_status()
    data = r.json()
    if not isinstance(data, list) or not data:
        raise ValueError("Estructura inesperada o vacía")
    log.info(f"Filas recibidas: {len(data)}")
    return data

@task
def load_to_staging(records: List[Dict[str, Any]]) -> dict:
    log = get_run_logger()
    batch_id = str(uuid.uuid4())
    rows = [(batch_id, json.dumps(rec)) for rec in records]
    with get_conn() as conn, conn.cursor() as cur:
        execute_values(
            cur,
            "INSERT INTO staging.raw_coingecko (batch_uuid, payload) VALUES %s",
            rows,
            template="(%s, %s::jsonb)",
            page_size=1000,
        )
    log.info(f"Inserted {len(rows)} rows into staging (batch={batch_id})")
    return {"batch_uuid": batch_id, "rows": len(rows)}

@task
def transform_to_bronze(batch_uuid: str):
    sql = """
    WITH src AS (
      SELECT
        payload->>'id'   AS asset_id,
        payload->>'symbol' AS symbol,
        payload->>'name'   AS name,
        NULLIF(payload->>'current_price','')::numeric  AS current_price,
        NULLIF(payload->>'market_cap','')::numeric     AS market_cap,
        NULLIF(payload->>'total_volume','')::numeric   AS total_volume,
        NULLIF(payload->>'high_24h','')::numeric       AS high_24h,
        NULLIF(payload->>'low_24h','')::numeric        AS low_24h,
        NULLIF(payload->>'price_change_percentage_1h_in_currency','')::numeric  AS pct_change_1h,
        NULLIF(payload->>'price_change_percentage_24h_in_currency','')::numeric AS pct_change_24h,
        NULLIF(payload->>'price_change_percentage_7d_in_currency','')::numeric  AS pct_change_7d,
        (payload->>'last_updated')::timestamptz        AS last_updated,
        now() AT TIME ZONE 'utc'                       AS run_ts_utc
      FROM staging.raw_coingecko
      WHERE batch_uuid = %s
    )
    INSERT INTO bronze.coingecko_markets (
        asset_id, symbol, name, current_price, market_cap, total_volume,
        high_24h, low_24h, pct_change_1h, pct_change_24h, pct_change_7d,
        last_updated, run_ts_utc
    )
    SELECT *
    FROM src
    WHERE asset_id IS NOT NULL AND last_updated IS NOT NULL
    ON CONFLICT (asset_id, last_updated) DO UPDATE SET
        symbol = EXCLUDED.symbol,
        name = EXCLUDED.name,
        current_price = EXCLUDED.current_price,
        market_cap = EXCLUDED.market_cap,
        total_volume = EXCLUDED.total_volume,
        high_24h = EXCLUDED.high_24h,
        low_24h  = EXCLUDED.low_24h,
        pct_change_1h = EXCLUDED.pct_change_1h,
        pct_change_24h = EXCLUDED.pct_change_24h,
        pct_change_7d = EXCLUDED.pct_change_7d,
        run_ts_utc = EXCLUDED.run_ts_utc;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, (batch_uuid,))

@task
def refresh_silver_daily():
    sql = """
    INSERT INTO silver.coingecko_daily_metrics (
      metric_date, asset_id, name, symbol, close_price, market_cap, total_volume
    )
    SELECT
      (last_updated AT TIME ZONE 'utc')::date AS metric_date,
      asset_id,
      name,
      symbol,
      current_price AS close_price,
      market_cap,
      total_volume
    FROM (
      SELECT *,
             ROW_NUMBER() OVER (
               PARTITION BY asset_id, (last_updated AT TIME ZONE 'utc')::date
               ORDER BY last_updated DESC
             ) AS rn
      FROM bronze.coingecko_markets
    ) t
    WHERE rn = 1
    ON CONFLICT (metric_date, asset_id) DO UPDATE SET
      name = EXCLUDED.name,
      symbol = EXCLUDED.symbol,
      close_price = EXCLUDED.close_price,
      market_cap = EXCLUDED.market_cap,
      total_volume = EXCLUDED.total_volume;
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql)

@flow(name="elt_coingecko_to_rds")
def run_elt():
    log = get_run_logger()
    raw = extract_raw()
    load_info = load_to_staging(raw)
    transform_to_bronze(load_info["batch_uuid"])
    refresh_silver_daily()
    log.info("ELT OK")
    return {"rows_raw": load_info["rows"], "batch_uuid": load_info["batch_uuid"]}

if __name__ == "__main__":
    run_elt()
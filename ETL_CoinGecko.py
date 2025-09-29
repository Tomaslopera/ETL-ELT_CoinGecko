from __future__ import annotations
import os
import io
from datetime import datetime, timezone
from typing import List, Dict, Any

import requests
import pandas as pd
import boto3

from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/markets"
    "?vs_currency=usd&order=market_cap_desc&per_page=100&page=1"
    "&price_change_percentage=1h,24h,7d"
)

# Extracción de datos API
@task(retries=3, retry_delay_seconds=10, cache_key_fn=task_input_hash, cache_expiration=None)
def extract_from_api(url: str = COINGECKO_URL) -> List[Dict[str, Any]]:
    logger = get_run_logger()
    logger.info(f"Solicitando datos: {url}")
    resp = requests.get(url, timeout=30)
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list) or len(data) == 0:
        raise ValueError("API devolvió estructura inesperada o vacía")
    logger.info(f"Filas recibidas: {len(data)}")
    return data

# Transformación de datos
@task
def transform(records: List[Dict[str, Any]]) -> pd.DataFrame:
    """Limpia y selecciona columnas relevantes."""
    logger = get_run_logger()
    df = pd.DataFrame.from_records(records)
    cols = [
        "id", "symbol", "name",
        "current_price", "market_cap", "total_volume",
        "high_24h", "low_24h",
        "price_change_percentage_1h_in_currency",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency",
        "last_updated"
    ]
    keep = [c for c in cols if c in df.columns]
    df = df[keep].copy()
    for c in [
        "current_price","market_cap","total_volume","high_24h","low_24h",
        "price_change_percentage_1h_in_currency",
        "price_change_percentage_24h_in_currency",
        "price_change_percentage_7d_in_currency"
    ]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    if "last_updated" in df.columns:
        df["last_updated"] = pd.to_datetime(df["last_updated"], errors="coerce")

    run_ts = datetime.now(timezone.utc)
    df["run_ts_utc"] = run_ts

    logger.info(f"DataFrame final: {df.shape[0]} filas x {df.shape[1]} cols")
    return df

# Conversión a Parquet
@task
def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False)
    return buf.getvalue()

# Carga a S3
@task
def upload_to_s3(data: bytes, key: str) -> str:
    logger = get_run_logger()
    bucket = "coingecko-prefect"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=bucket, Key=key, Body=data)
    uri = f"s3://{bucket}/{key}"
    logger.info(f"Subido: {uri}")
    return uri

# Flujo ETL completo
@flow(name="etl_coingecko_to_s3")
def etl_coingecko_to_s3():
    logger = get_run_logger()

    prefix = os.getenv("S3_PREFIX", "etl/crypto_markets")

    raw = extract_from_api()

    df = transform(raw)

    pq_bytes = to_parquet_bytes(df)

    run_dt = datetime.now(timezone.utc)
    date_part = run_dt.strftime("%Y/%m/%d")
    ts_part = run_dt.strftime("%Y%m%dT%H%M%SZ")
    key = f"{prefix}/dt={date_part}/coingecko_top100_{ts_part}.parquet"

    uri = upload_to_s3(pq_bytes, key=key)

    logger.info(f"ETL OK: {uri}")
    return {"rows": len(df), "s3_uri": uri}

if __name__ == "__main__":
    etl_coingecko_to_s3()
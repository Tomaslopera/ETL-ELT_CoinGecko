-- Esquemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Tabla cruda (EL)
CREATE TABLE IF NOT EXISTS staging.raw_coingecko (
  batch_uuid   UUID        NOT NULL,
  ingested_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload      JSONB       NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_raw_batch ON staging.raw_coingecko(batch_uuid);
CREATE INDEX IF NOT EXISTS idx_raw_id    ON staging.raw_coingecko ((payload->>'id'));

-- Bronze (flatten + tipos) con UPSERT por activo/ts
CREATE TABLE IF NOT EXISTS bronze.coingecko_markets (
  asset_id        TEXT        NOT NULL,
  symbol          TEXT,
  name            TEXT,
  current_price   NUMERIC,
  market_cap      NUMERIC,
  total_volume    NUMERIC,
  high_24h        NUMERIC,
  low_24h         NUMERIC,
  pct_change_1h   NUMERIC,
  pct_change_24h  NUMERIC,
  pct_change_7d   NUMERIC,
  last_updated    TIMESTAMPTZ NOT NULL,
  run_ts_utc      TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (asset_id, last_updated)
);
CREATE INDEX IF NOT EXISTS idx_bronze_last_updated ON bronze.coingecko_markets(last_updated);

-- Silver diario (tabla con UPSERT por día/activo)
CREATE TABLE IF NOT EXISTS silver.coingecko_daily_metrics (
  metric_date   DATE      NOT NULL,
  asset_id      TEXT      NOT NULL,
  name          TEXT,
  symbol        TEXT,
  close_price   NUMERIC,
  market_cap    NUMERIC,
  total_volume  NUMERIC,
  PRIMARY KEY (metric_date, asset_id)
);



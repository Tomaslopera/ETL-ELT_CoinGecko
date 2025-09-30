# ETL/ELT CoinGecko: Prefect

Este proyecto implementa un **flujo ETL (Extract, Transform, Load)** utilizando [Prefect](https://www.prefect.io/). El flujo extrae información financiera desde la API pública de [CoinGecko](https://www.coingecko.com/), transforma los datos con **pandas** y los carga en formato **Parquet** a un bucket de **Amazon S3**.

## Flujo ETL

1. **Extract**  
   - Fuente: API pública de CoinGecko (`/coins/markets`).
   - Datos: top 50/100 criptoactivos ordenados por capitalización de mercado (USD).

2. **Transform**  
   - Selección de columnas relevantes (precio, market cap, volumen, variaciones 1h/24h/7d).
   - Limpieza de tipos y fechas.
   - Agregado de un timestamp de ejecución.

3. **Load**  
   - Serialización a Parquet en memoria.
   - Carga al bucket **S3** (`coingecko-prefect`), organizado por fecha de ejecución:
     ```
     s3://coingecko-prefect/etl/crypto_markets/dt=YYYY/MM/DD/coingecko_top50_TIMESTAMP.parquet
     ```

## Flujo ELT

1. **Extract**  
   - Fuente: API pública de CoinGecko (`/coins/markets`).
   - Datos: top 50/100 criptoactivos ordenados por capitalización de mercado (USD).
   - Destino inmediato: inserción de los registros crudos en la tabla staging.raw_coingecko en RDS (JSONB + batch_uuid + timestamp de ingesta).

2. **Load**  
   - Los datos se cargan sin transformar en la capa staging de la base de datos.
   - Cada corrida genera un nuevo lote identificado por batch_uuid, lo que permite auditoría y reprocesamiento.

3. **Transform**  
Se ejecutan transformaciones directamente en PostgreSQL (capa bronze y silver):
   - Bronze (bronze.coingecko_markets):
      - Flatten del JSON crudo.
      - Tipado numérico y de fechas.
      - Inserción con UPSERT por (asset_id, last_updated) para idempotencia.
	- Silver (silver.coingecko_daily_metrics):
	   - Agregación de “cierre diario” por activo (ROW_NUMBER() con último snapshot de cada día).
      - Inserción con UPSERT por (metric_date, asset_id) para mantener consistencia diaria.

## Instalación

```bash
  git clone https://github.com/Tomaslopera/ETL_CoinGecko
```
```bash
  pip install prefect pandas boto3 pyarrow
```

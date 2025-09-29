# ETL CoinGecko: Prefect

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

## Instalación

```bash
  git clone https://github.com/Tomaslopera/ETL_CoinGecko
```
```bash
  pip install prefect pandas boto3 pyarrow
```

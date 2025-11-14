We implemented a secure, reproducible stock analytics workflow using Airflow, yfinance, and Snowflake. An ETL DAG ingests OHLCV data into RAW.STOCK_PRICES. A second DAG trains a Snowflake-native SNOWFLAKE.ML.FORECAST model and writes predictions to MODEL.FORECASTS. A final table, ANALYTICS.FINAL_PRICES_FORECAST, unions actuals and forecasts for downstream visualization. All Snowflake credentials (account, user, password, role, warehouse, database) are stored only in Airflow Connections, and pipeline parameters are managed via Airflow Variables.



Stock Analytics Pipeline — Airflow × Snowflake × yfinance

Secure, reproducible pipeline that ingests OHLCV data, trains a Snowflake-native forecast model, and serves a finalized table for downstream BI.

✨ Highlights

Modular Airflow pipeline with separate DAGs for ETL, ML training, and finalization.

Idempotent loads using Snowflake MERGE for safe re-runs/backfills.

MultiIndex-safe ingestion: normalizes/flattens yfinance’s possible MultiIndex columns to clean, single-level, snake_case fields.

Snowflake-native ML via SNOWFLAKE.ML.FORECAST writing predictions to MODEL.FORECASTS.

Analytics-ready union of actuals + forecasts in ANALYTICS.FINAL_PRICES_FORECAST.

Strong security posture: all Snowflake credentials live in Airflow Connections; pipeline params live in Airflow Variables.

🧱 Tech Stack

Orchestration: Apache Airflow

Data Source: yfinance (historical OHLCV)

Warehouse & ML: Snowflake (RAW, MODEL, ANALYTICS schemas; SNOWFLAKE.ML.FORECAST)

Storage pattern: Idempotent MERGE upserts, re-runnable tasks

🔄 Data Flow
flowchart LR
    A[yfinance OHLCV] --> B[Airflow ETL DAG]
    B --> C[Snowflake RAW.STOCK_PRICES]
    C --> D[Airflow ML DAG]
    D --> E[Snowflake SNOWFLAKE.ML.FORECAST]
    E --> F[MODEL.FORECASTS]
    C --> G[Finalization DAG]
    F --> G
    G --> H[ANALYTICS.FINAL_PRICES_FORECAST]
    H --> I[Dashboards / Notebooks / BI]

🗂️ Schemas & Tables

RAW.STOCK_PRICES – normalized OHLCV with symbol, date, open, high, low, close, adj_close, volume.

MODEL.FORECASTS – model outputs (symbol, forecast_date, target_date, yhat, yhat_lower, yhat_upper, model_meta).

ANALYTICS.FINAL_PRICES_FORECAST – unioned actuals + forecasts for easy consumption.
<img width="1048" height="570" alt="image" src="https://github.com/user-attachments/assets/776cca2a-7eba-4fc6-9e28-a71b0a214037" />

🧩 Airflow DAGs

ETL DAG – Ingests daily OHLCV from yfinance → RAW.STOCK_PRICES

Flattens MultiIndex columns to single-level schema

Uses MERGE keyed by (symbol, date) for idempotency

ML DAG – Trains Snowflake-native forecast model

SNOWFLAKE.ML.FORECAST training/inference

Writes predictions → MODEL.FORECASTS

Finalization DAG – Builds analytics-ready table

Unions actuals and forecasts

Publishes → ANALYTICS.FINAL_PRICES_FORECAST

Each DAG is parameterized with Airflow Variables; secrets live in Airflow Connections.

🔐 Configuration
Airflow Connection (Secrets only)

Create a Snowflake connection (example):

Conn Id: snowflake_default

Conn Type: Snowflake

Account / User / Password / Role / Warehouse / Database: set securely here

Do not put credentials in Variables.

Airflow Variables (Pipeline parameters)

Recommended keys (examples—tune to your needs):

symbols: ["AAPL","MSFT","AMZN"]

start_date: "2010-01-01"

forecast_horizon_days: 30

retrain_schedule_cron: "0 2 * * *"

finalize_schedule_cron: "15 3 * * *"

🧪 Idempotent Load (Example MERGE)
MERGE INTO RAW.STOCK_PRICES AS t
USING (SELECT :symbol AS symbol, :date::DATE AS date, :open, :high, :low, :close, :adj_close, :volume) s
ON t.symbol = s.symbol AND t.date = s.date
WHEN MATCHED THEN UPDATE SET
  open = s.open, high = s.high, low = s.low, close = s.close,
  adj_close = s.adj_close, volume = s.volume, _updated_at = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN INSERT (symbol, date, open, high, low, close, adj_close, volume, _ingested_at)
VALUES (s.symbol, s.date, s.open, s.high, s.low, s.close, s.adj_close, s.volume, CURRENT_TIMESTAMP());

🚀 Run Locally
# 1) Start/Restart services
docker compose down && docker compose up -d

# 2) Open Airflow UI
# typically http://localhost:8080

# 3) Trigger the DAGs manually on first run
# ETL -> ML -> Finalization

🧰 Project Structure (sample)
.
├─ dags/
│  ├─ etl_yfinance_to_raw.py
│  ├─ train_snowflake_forecast_model.py
│  └─ finalize_prices_forecast.py
├─ sql/
│  ├─ create_raw_tables.sql
│  ├─ merge_raw_prices.sql
│  └─ build_final_prices_forecast.sql
├─ notebooks/
│  └─ exploration_and_bi_examples.ipynb
├─ docker-compose.yaml
└─ README.md

📊 Data Model Notes

yfinance sometimes returns MultiIndex columns (e.g., when fetching multiple tickers/fields).

The ETL DAG normalizes columns to a clean, single-level schema and snake_case naming to ensure stable Snowflake DDL & MERGE keys.

📈 Consuming the Data

Point dashboards or notebooks to:

ANALYTICS.FINAL_PRICES_FORECAST – unified table for actuals + predictions

Typical dimensions: symbol, date/target_date

Typical measures: close, adj_close, yhat, yhat_lower, yhat_upper

✅ Operational Characteristics

Re-runnable & auditable: deterministic tasks, lineage from RAW → MODEL → ANALYTICS

Clear separation of concerns across DAGs


Stock Analytics Pipeline — Airflow + Snowflake + yfinance
Abstract

This project implements a secure, reproducible stock analytics system. An Airflow ETL DAG ingests daily OHLCV from yfinance, normalizes any MultiIndex columns to a flat schema, and upserts into Snowflake with MERGE for idempotency. A second DAG trains a Snowflake-native SNOWFLAKE.ML.FORECAST model and writes predictions to MODEL.FORECASTS. A final table, ANALYTICS.FINAL_PRICES_FORECAST, unions actuals and forecasts for direct BI/Notebook consumption. Secrets live in Airflow Connections; runtime parameters live in Variables.

Highlights

Two DAGs: yfinance_etl (RAW load) and ml_forecast (train → predict → finalize).

Idempotent loads: Snowflake MERGE keyed by (SYMBOL, TS); safe re-runs & backfills.

Schema discipline: RAW, MODEL, ANALYTICS with clear lineage and atomic transactions.

MultiIndex-safe ingestion: flattens yfinance columns to single-level, snake_case.

Configurable: symbols, lookback, and horizon controlled via Airflow Variables.

Security: account/user/password/role/warehouse/database stored only in Airflow Connections.

Architecture
<img width="1016" height="552" alt="image" src="https://github.com/user-attachments/assets/34f51d28-44ab-4836-8bc7-c4f8b9d11522" />



Snowflake Model

RAW.STOCK_PRICES
Columns: SYMBOL, TS, OPEN, HIGH, LOW, CLOSE, ADJ_CLOSE, VOLUME, LOAD_TS
Key: (SYMBOL, TS)

MODEL.FORECASTS
Columns: SYMBOL, TS, PREDICTED_CLOSE, MODEL_NAME, TRAINED_AT, HORIZON_D, LOAD_TS
Key: (SYMBOL, TS, MODEL_NAME)

ANALYTICS.FINAL_PRICES_FORECAST
Union of ACTUAL (from RAW) and FORECAST (from MODEL) with SOURCE flag.

Quickstart

Prereqs: Airflow (Docker OK), Snowflake account, packages: yfinance, snowflake-connector-python, apache-airflow-providers-snowflake.

Airflow Connection (Secrets):
Create snowflake_default (type: Snowflake). Extra JSON example:

{"account":"<acct>","warehouse":"<wh>","database":"<db>","schema":"RAW","role":"<role>"}


Airflow Variables (Params):

stock_symbols: ["AAPL","MSFT","TSLA"]

lookback_days: 365

forecast_horizon_days: 14

target_schema_raw: "RAW", target_schema_model: "MODEL", target_schema_analytics: "ANALYTICS"

Run order: Trigger yfinance_etl → trigger ml_forecast.

Operational Notes

ETL writes to a staging/temp object then performs a single MERGE into RAW for consistency.

Forecast DAG: CREATE OR REPLACE MODEL → PREDICT → MERGE into MODEL → rebuild ANALYTICS table.

Designed for daily scheduling; backfills supported via variable overrides.

Consuming the Data

Query ANALYTICS.FINAL_PRICES_FORECAST:

Dimensions: SYMBOL, TS, SOURCE (ACTUAL|FORECAST)

Measures: CLOSE, PREDICTED_CLOSE (yhat equivalent), optional intervals if configured

Repo Layout (suggested)
dags/
  yfinance_etl.py
  ml_forecast.py
sql/
  bootstrap_ddl.sql
  analytics_checks.sql
docs/
  screenshots/


TL;DR — Airflow orchestrates ingestion and Snowflake-native forecasting; outputs a single analytics table that blends actuals and predictions for downstream use.

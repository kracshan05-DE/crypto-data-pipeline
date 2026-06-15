Crypto Data Pipeline 🚀

An end-to-end data engineering pipeline that ingests live cryptocurrency prices, transforms the data using dbt, and orchestrates everything with Apache Airflow — built entirely on GCP free tier.


Architecture

CoinGecko API
      │
      ▼
Python Ingestion Script
      │
      ▼
Google Cloud Storage (Raw JSON)
      │
      ▼
BigQuery — crypto_raw.raw_prices
      │
      ▼
dbt Transformations
      │
      ▼
BigQuery — crypto_transformed.mart_daily_summary
      │
      ▼
Apache Airflow (Hourly Orchestration)


Tech Stack

LayerToolIngestionPython, CoinGecko APIRaw StorageGoogle Cloud Storage (GCS)Data WarehouseBigQueryTransformationdbt (data build tool)OrchestrationApache AirflowVisualizationLooker StudioCloudGCP (asia-south1)Version ControlGit / GitHub


Project Structure

crypto-data-pipeline/
├── dags/
│   └── crypto_pipeline_dag.py           # Airflow DAG — hourly schedule
├── ingestion/
│   └── fetch_crypto_prices.py           # Fetches BTC, ETH, BNB, SOL from CoinGecko
├── crypto_dbt/
│   ├── models/
│   │   ├── staging/
│   │   │   └── stg_crypto_prices.sql    # Cleans and types raw data
│   │   └── marts/
│   │       └── mart_daily_summary.sql   # Daily OHLC-style summary per coin
│   ├── dbt_project.yml
│   └── profiles.yml
├── requirements.txt
└── README.md


Data Flow

1. Ingestion

The Python script calls the CoinGecko /simple/price endpoint every hour and fetches live prices for BTC, ETH, BNB, and SOL. Raw JSON responses are stored in GCS with a timestamped path:

gs://crypto-pipeline-raw-data-kracshan/raw/prices/YYYY/MM/DD/HH/prices.json

2. Loading to BigQuery

Data is loaded from GCS into BigQuery's raw layer:


Dataset: crypto_raw
Table: raw_prices


3. dbt Transformations

dbt transforms raw data through two layers:

Staging — stg_crypto_prices.sql

Reads from crypto_raw.raw_prices and produces a clean, typed dataset:


Casts price_usd and volume_24h to NUMERIC
Casts market_cap to INT64
Parses fetched_at into both TIMESTAMP and DATE (price_date)
Filters out rows where price_usd IS NULL


sql
SELECT
    coin_id,
    symbol,
    name,
    CAST(price_usd AS NUMERIC)        AS price_usd,
    CAST(market_cap AS INT64)         AS market_cap,
    CAST(volume_24h AS NUMERIC)       AS volume_24h,
    CAST(price_change_24h AS NUMERIC) AS price_change_24h,
    TIMESTAMP(fetched_at)             AS fetched_at,
    DATE(fetched_at)                  AS price_date
FROM {{ source('crypto_raw', 'raw_prices') }}
WHERE price_usd IS NOT NULL

Mart — mart_daily_summary.sql

Reads from stg_crypto_prices and aggregates into a daily summary per coin:


avg_price — average price across all hourly fetches that day
low_price / high_price — daily price range
daily_range — spread between high and low (volatility indicator)
avg_change_24h — average 24h price change sentiment
data_points — number of hourly records collected that day


sql
SELECT
    price_date,
    symbol,
    name,
    ROUND(AVG(price_usd), 2)                    AS avg_price,
    ROUND(MIN(price_usd), 2)                    AS low_price,
    ROUND(MAX(price_usd), 2)                    AS high_price,
    ROUND(MAX(price_usd) - MIN(price_usd), 2)   AS daily_range,
    ROUND(AVG(price_change_24h), 2)             AS avg_change_24h,
    COUNT(*)                                     AS data_points
FROM {{ ref('stg_crypto_prices') }}
GROUP BY 1, 2, 3
ORDER BY 1 DESC, 2

4. Orchestration

Apache Airflow runs the full pipeline on an hourly schedule:


Fetch prices from CoinGecko
Upload raw JSON to GCS
Load GCS → BigQuery
Trigger dbt transformations



Setup & Running Locally

Prerequisites


GCP account (free tier works)
Python 3.8+
Google Cloud SDK


1. Clone the repo

bashgit clone https://github.com/kracshan05-DE/crypto-data-pipeline.git
cd crypto-data-pipeline

2. Install dependencies

bashpip install -r requirements.txt

3. Configure GCP

bashgcloud auth application-default login
gcloud config set project crypto-pipeline-498217

4. Set up dbt

bashcd crypto_dbt
dbt deps
dbt run

5. Start Airflow

bashexport AIRFLOW_HOME=~/airflow
airflow db migrate
airflow standalone

Then open http://localhost:8080 and trigger the crypto_pipeline DAG.


Key Learnings


dbt enforces layered thinking — separating raw, staging, and mart layers keeps transformations clean, testable, and maintainable
Airflow is unforgiving with imports — always verify provider package versions; wrong import paths cause silent DAG failures
GCP free tier is production-capable — BigQuery sandbox + GCS free tier is more than enough to build and run a real pipeline
Cloud Shell eliminates auth friction — developing directly in Cloud Shell means no service account keys, no admin rights needed on local machine



What's Next


 Add Looker Studio dashboard (in progress)
 Expand to more coins and historical backfill
 Add dbt tests and data quality checks
 Set up CI/CD with GitHub Actions
 Add streaming layer with Pub/Sub



Author

Racshan Chandrakumar

Data Engineer | Sri Lanka

LinkedIn · GitHub
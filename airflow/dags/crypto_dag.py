from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "kracshan05-DE",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="crypto_pipeline",
    default_args=default_args,
    description="Hourly crypto price ingestion to BigQuery",
    schedule="@hourly",
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["crypto", "portfolio", "bigquery"],
) as dag:

    ingest = BashOperator(
        task_id="ingest_from_coingecko",
        bash_command="cd ~/crypto-pipeline && python3 scripts/ingest_crypto.py",
    )

    verify = BashOperator(
        task_id="verify_bq_load",
        bash_command="bq query --use_legacy_sql=false 'SELECT COUNT(*) as row_count FROM `crypto-pipeline-498217.crypto_raw.raw_prices`'",
    )

    ingest >> verify
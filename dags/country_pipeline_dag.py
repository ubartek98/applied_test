from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from etl.load_countries_data import extract_and_load_to_bq

# --- Default arguments ---
default_args = {
    "owner": "bartosz.urban",
    "depends_on_past": False,
    "email": ["bartosz.urban@rtbhouse.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# --- DAG definition ---
with DAG(
    dag_id="country_pipeline_dag",
    default_args=default_args,
    description="Daily ETL: Load country data from REST API into BigQuery bronze layer",
    schedule_interval="0 6 * * *",  # runs daily at 06:00 UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["applied"],
) as dag:

    load_countries_to_bq = PythonOperator(
        task_id="extract_and_load_countries",
        python_callable=extract_and_load_to_bq,
    )

    load_countries_to_bq

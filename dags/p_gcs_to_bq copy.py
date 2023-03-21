import os


from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator



PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'chicago_crime_all')




default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}



dag = DAG(
    'load_csv_to_bq',
    default_args=default_args,
    description='Load CSV files from GCS to BigQuery',
    catchup=False,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['dtc-de'],
)

def load_csv_to_bq():
    gcs_hook = GoogleCloudStorageHook()
    bq_hook = BigQueryHook()

    # Load CSV files from GCS bucket
    bucket = BUCKET
    file_pattern = 'project/*.csv'
    files = gcs_hook.list(bucket, prefix=file_pattern)
    source_uris = [f"gs://{bucket}/{f}" for f in files]

    # Set up BigQuery configuration
    table = f'{BIGQUERY_DATASET}.my-bq-table'
    schema = [
        {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'column2', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'column3', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'column4', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        # ...
    ]
    write_disposition = 'WRITE_TRUNCATE'

    # Load data into BigQuery
    bq_hook.insert_rows_from_files(table, source_uris, schema=schema, write_disposition=write_disposition)

load_csv_to_bq_task = PythonOperator(
    task_id='load_csv_to_bq',
    python_callable=load_csv_to_bq,
    dag=dag,
)

load_csv_to_bq_task
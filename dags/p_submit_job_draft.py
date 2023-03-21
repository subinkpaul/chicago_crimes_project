from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
import os

BUCKET = os.environ.get("GCP_GCS_BUCKET")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BIGQUERY_DATASET='chicago_crime_all'
INPUT_TABLE_NAME = 'et_chicago_crimes'
CRIME_TABLE = 'CRIME_TYPE_COUNT'
CRIMES_DAILY_TABLE = 'CRIMES_DAILY'

# Define the Dataproc cluster name and region
cluster_name = 'my-dataproc-cluster'
region = 'europe-west6'

# Create a DAG object
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG('project_spark_processing', default_args=default_args, schedule_interval=timedelta(days=1))



# Define the PySpark job
pyspark_job = {
    'reference': {
        'project_id': PROJECT_ID,
        'job_id': 'transform_bq_data1'
    },
    'placement': {
        'cluster_name': cluster_name
    },
    'pyspark_job': {
        'main_python_file_uri': f'gs://{BUCKET}/spark_job.py',
        'jar_file_uris': ['gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.29.0.jar'],
        'args': [
            '--input_table', f'{PROJECT_ID}.{BIGQUERY_DATASET}.{INPUT_TABLE_NAME}',
            '--output_table_crime', f'{BIGQUERY_DATASET}.{CRIME_TABLE}',
            '--output_table_daily', f'{BIGQUERY_DATASET}.{CRIMES_DAILY_TABLE}'
        ]
    }
}

# Define the PySpark job
process_data_job = DataprocSubmitJobOperator(
    task_id='process_data',
    job=pyspark_job,
    region=region,
    dag=dag
)




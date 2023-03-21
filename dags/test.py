from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.utils.dates import days_ago
import os

# Define the DAG start date
dag_start_date = days_ago(1)
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
# Define the DAG and its configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': dag_start_date,
    'retries': 1
}

dag = DAG(
    dag_id='submit_pyspark_job',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

# Define the PySpark job
pyspark_job = {
    'reference': {
        'project_id': PROJECT_ID,
        'job_id': 'your_job_id'
    },
    'placement': {
        'cluster_name': 'your_cluster_name'
    },
    'pyspark_job': {
        'main_python_file_uri': 'gs://your-bucket/your-script.py'
    }
}

# Define the DataprocSubmitJobOperator
submit_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job',
    job=pyspark_job,
    region='europe-west1',
    dag=dag
)

# Set task dependencies
submit_job


from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp
import os

def my_function():
    PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
    BIGQUERY_DATASET='chicago_crime_all'
    INPUT_TABLE_NAME = 'et_chicago_crimes_lite'

    print("Creating Spark session")

    # create a Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("bigquery-to-pyspark") \
        .getOrCreate()


    print("Spark session created")
    # set GCP credentials for accessing BigQuery
    spark.conf.set("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest.jar")
    spark.conf.set("google.cloud.auth.service.account.enable", "true")
    spark.conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    spark.conf.set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "/path/to/keyfile.json")

    # define the project ID and dataset ID of the BigQuery table to read from
    project_id = PROJECT_ID
    dataset_id = BIGQUERY_DATASET
    table_id = INPUT_TABLE_NAME

    output_table=f"{project_id}.{dataset_id}.TOTAL_CRIMES"

    # read the BigQuery table into a PySpark DataFrame
    df = spark.read \
        .format("bigquery") \
        .option("project", project_id) \
        .option("dataset", dataset_id) \
        .option("table", table_id) \
        .load()

    # perform any necessary data transformations on the DataFrame
    # for example, you could filter rows, group by columns, or rename columns
    output_df = df.groupBy("Primary_Type").count()

    # write the transformed DataFrame back to BigQuery
    output_df.write \
        .format("bigquery") \
        .option("table", output_table) \
        .option("temporaryGcsBucket", "your-gcs-bucket") \
        .mode("overwrite") \
        .save()

    print('Data written to BQ table')

    # Stop the SparkSession
    spark.stop()


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

dag = DAG(
    'spark_local_dag',
    default_args=default_args,
    schedule_interval=None,
)


call_script = PythonOperator(
    task_id='spark_processing',
    python_callable=my_function,
    dag=dag,
)

call_script
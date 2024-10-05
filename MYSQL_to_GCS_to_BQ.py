from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.mysql_to_gcs import MySQLToGCSOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("mysql_to_gcs", start_date=datetime(2024, 9, 1),
    schedule_interval="@daily", catchup=False) as dag:

    mysql_to_gcs_export = MySQLToGCSOperator(
        task_id="mysql_to_gcs_export",
        mysql_conn_id="mysql_conn",
        sql="SELECT * FROM test1",
        bucket="dag_preview",
        filename="mysql_to_gcs.csv",
        export_format="csv",
    )

    [mysql_to_gcs_export]

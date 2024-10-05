from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("postgres_to_gcs", start_date=datetime(2024, 9, 1),
    schedule_interval="@daily", catchup=False) as dag:

    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id="postgres_conn",
        sql="select  * from dwh.tb_pkmn_clean",
        bucket="dag_preview",
        filename="postgres_to_gcs.csv",
        export_format="csv",
    )

    [postgres_to_gcs]

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryToGCSOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("bigquery_to_gcs", start_date=datetime(2024, 9, 1),
    schedule_interval="@daily", catchup=False) as dag:

    bigquery_to_gcs = BigQueryToGCSOperator(
        task_id="bigquery_to_gcs",
        source_project_dataset_table=f"dwh-dtp.bdpkmn.tb_gcloud2",
        destination_cloud_storage_uris=[f"gs://dag_preview/file_dwh-dtp_bdpkmn_tb_gcloud2"],
    )

    [bigquery_to_gcs]

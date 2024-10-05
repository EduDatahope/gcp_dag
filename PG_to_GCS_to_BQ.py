from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("postgres_to_gcs", start_date=datetime(2024, 9, 1),
    schedule_interval="@daily", catchup=False) as dag:

    postgres_to_gcs = PostgresToGCSOperator(
        task_id="postgres_to_gcs",
        postgres_conn_id="postgres_conn",
        sql="select  * from dwh.tvw_pkmn_clean",
        bucket="dag_preview",
        filename="postgres_to_gcs.csv",
        field_delimiter=";"
        export_format="csv",
    )

    load_csv_delimiter = GCSToBigQueryOperator(
        task_id="gcs_to_bigquery_example_delimiter_async",
        bucket="dag_preview",
        source_objects=["postgres_to_gcs.csv"],
        source_format="csv",
        destination_project_dataset_table=f"dwh-dtp.bdpkmn.postgres_to_bq",
        write_disposition="WRITE_TRUNCATE",
        external_table=False,
        autodetect=True,
        field_delimiter=";",
        quote_character="",
    )

    postgres_to_gcs >> load_csv_delimiter

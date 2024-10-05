from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("gcp_job_sp_ex1", start_date=datetime(2024, 9, 1),
    schedule_interval="@daily", catchup=False) as dag:

     gcp_job_sp_ex1 = BigQueryInsertJobOperator(
     task_id="gcp_job_sp_ex1",
     configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load1`('comp**ex1'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )


     [gcp_job_sp_ex1]

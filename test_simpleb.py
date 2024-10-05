from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("gcp_job_exec2", start_date=datetime(2024, 9, 1),
    schedule_interval="@daily", catchup=False) as dag:

     call_stored_procedure = BigQueryInsertJobOperator(
     task_id="call_stored_procedure2",
     configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load2`('comp$$$$$'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )

     [call_stored_procedure2]

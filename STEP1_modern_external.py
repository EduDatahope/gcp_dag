from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("gcp_job_sp_ex1", start_date=datetime(2024, 10, 5),
    schedule_interval="*/45 * * * *", catchup=False) as dag:

     step_gcp_job_sp_ex1 = BigQueryInsertJobOperator(
     task_id="step_gcp_job_sp_ex1",
     configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load1`('comp**ex1'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )

     @task
     def done():
       print("done :)")

     step_gcp_job_sp_ex1 >> done()

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


with DAG("gcp_job_sp_ex2", start_date=datetime(2024, 10, 5),
    schedule_interval="*/45 * * * *", catchup=False) as dag:

     step_gcp_job_sp_ex2 = BigQueryInsertJobOperator(
     task_id="step_gcp_job_sp_ex2",
     configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load2`('comp**ex2'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )

     waiting_for_1 = ExternalTaskSensor(
        task_id = 'waiting_for_1',
        external_dag_id = 'gcp_job_sp_ex1',
        external_task_id = None,
        timeout=300)

     @task
     def done():
        print("done")

     waiting_for_1 >> step_gcp_job_sp_ex2 >> done()

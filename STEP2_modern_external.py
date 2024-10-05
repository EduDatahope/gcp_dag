from airflow.decorators import dag , task
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from random import randint
from datetime import datetime


@dag(start_date=datetime(2024, 9, 1),schedule="@daily", catchup=False)
def dag_step2():

     @task
     def call_stored_procedure2():
      BigQueryInsertJobOperator(
      task_id="call_stored_procedure",
      configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load2`('comp2**ex'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )
     call_stored_procedure2()

dag_step2()

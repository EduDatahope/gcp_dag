from airflow.decorators import dag , task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

@dag(start_date=datetime(2024, 9, 1),schedule="@daily", catchup=False)
def dag_step1():

     @task
     def call_stored_procedure1():
      BigQueryInsertJobOperator(
      task_id="call_stored_procedure1",
      configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load1`('comp1**ex'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )
     ##comentario si fueran varios pasos
     ##seria call_stored_procedure1() >> call_stored_procedure2()
     call_stored_procedure1()

dag_step1()

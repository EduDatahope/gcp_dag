from airflow.decorators import dag , task
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

@dag(start_date=datetime(2024, 9, 1),schedule="@daily", catchup=False)
def dag_step2():

     waiting_for_1 = ExternalTaskSensor(
        task_id = 'waiting_for_1',
        external_dag_id = 'dag_step1',
        external_task_id = 'call_stored_procedure1')

     @task
     def call_stored_procedure2():
      BigQueryInsertJobOperator(
      task_id="call_stored_procedure2",
      configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load2`('comp2**ex'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )

     waiting_for_1 >> call_stored_procedure2()

dag_step2()

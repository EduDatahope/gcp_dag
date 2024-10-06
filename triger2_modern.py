from airflow.decorators import dag , task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', description ='venta_step2_canal' ,
    tags = ['etl venta'] , catchup=False)
def triger_2_modern():

    @task
    def start2():
     BigQueryInsertJobOperator(
     task_id="step_gcp_job_m2",
     configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load2`('trg2**'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )

    start2()

triger_2_modern()

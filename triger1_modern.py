from airflow.decorators import dag , task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

@dag(start_date=datetime(2023, 1, 1), schedule='@daily', description ='venta_step1_extract' ,
    tags = ['etl venta'] , catchup=False)
def triger_1_modern():

	@task
	def start1():
	 BigQueryInsertJobOperator(
     task_id="step_gcp_job_m1",
     configuration={
        "query": {
            "query": "CALL `dwh-dtp.bdpkmn.sp_load1`('trg1**'); ",
            "useLegacySql": False,
             }
             },
     location='US',
     )

	trigger = TriggerDagRunOperator(
		task_id='trigger_target_dag',
		trigger_dag_id='triger_2_modern',
		conf={"message": "my_data"},
		wait_for_completion=True
	)

	@task
	def done():
		print("done")

	start1() >> trigger >> done()

triger_1_modern()

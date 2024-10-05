from airflow.decorators import dag , task
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime


@dag(start_date=datetime(2024, 9, 1),schedule="@daily", catchup=False)
def waiting_step_1_2():



    waiting_for_2 = ExternalTaskSensor(
        task_id = 'waiting_sp2',
        external_dag_id = 'dag_step2',
        external_task_id = 'call_stored_procedure2')

    @task
    def final_msg():
        print("done")

    waiting_for_2 >> final_msg()

waiting_step_1_2()

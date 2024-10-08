import base64
from typing import Any
import airflow_api


def airflow_invoker(event, context):
    """
    Se debe crear el file .py con airflow_api
    el valor del web server es del airflow
    """
    web_server_url = (
        "https://d2c4d3bf3ed74cab8ad2dfe1131d9854-dot-us-central1.composer.googleusercontent.com"
    )
    # Replace with the ID of the DAG that you want to run.
    dag_id = 'dataproc_workflow_dag'
    airflow_api.trigger_dag(web_server_url, dag_id, event)

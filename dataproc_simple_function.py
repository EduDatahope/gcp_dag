import re

from google.cloud import dataproc_v1
from google.cloud import storage

project_id = 'dwhp-437913'
region = 'us-central1'
cluster_name ='cluster-b4cd'
gcs_bucket = 'devops-python-dhope'
spark_filename = 'simple_bq_example.py'

# Create the job client.
job_client = dataproc_v1.JobControllerClient(
    client_options={"api_endpoint": f"{region}-dataproc.googleapis.com:443"}
)

# Create the job config.
job = {
    "placement": {"cluster_name": cluster_name},
    "pyspark_job": {"main_python_file_uri": f"gs://{gcs_bucket}/{spark_filename}"},
}

operation = job_client.submit_job_as_operation(
    request={"project_id": project_id, "region": region, "job": job}
)

response = operation.result()

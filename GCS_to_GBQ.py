from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'GCS_to_GBQ',
    default_args=default_args,
    schedule_interval=None,
    tags=['GCS_to_GBQ'],
) as dag:
    gcs_to_bq = GCSToBigQueryOperator(
        task_id='gcs_to_bq',
        bucket='source_data_comp', # GCS bucket name
        source_objects=['tb_pokemon_clean.csv'], 
        source_format='CSV',
	field_delimiter=';',
        destination_project_dataset_table='dwh-dtp.bdpkmn.tb_comp_pkmn_stat', # GBQ table name
        schema_fields=[
            {'name': 'cod_pok', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'desc_pok', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'type_pok', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'attack', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'defense', 'type': 'STRING', 'mode': 'NULLABLE'},
	    {'name': 'special_attack', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
        skip_leading_rows=1,
        autodetect= False,
    )

    gcs_to_bq


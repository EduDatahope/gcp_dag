import time
from datetime import datetime
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
import pandas as pd
from google.oauth2 import service_account

# Declare Dag
@dag(schedule_interval="0 10 * * *", start_date=datetime(2024, 9, 5), catchup=False, tags=['load_gcp'])
# Define Dag Function
def extract_and_load():
# Define tasks
    @task()
    def sql_extract():
        try:
            hook = MsSqlHook(mssql_conn_id="sqlserver")
            sql = """ SELECT TOP 1 [cod_pok] FROM [sqlagro2].[dbo].[tb_pokemon_clean] """
            df = hook.get_pandas_df(sql)
            # print(df.head())
            print(df)
            tbl_dict = df.to_dict('dict')
            return tbl_dict
        except Exception as e:
            print("Data extract error: " + str(e))
    #
    @task()
    def gcp_load(tbl_dict: dict):
        #
        try:

            credentials = service_account.Credentials.from_service_account_info(Variable.get(key="apikey",deserialize_json=True))
            project_id = "dwh-dtp"
            dateset_ref = "bdpkmn"
            table_ref = "tb_sql_pkmn_stat"

            # 
            for value in tbl_dict.values():
                #print(value)
                val = value.values()
                for v in val:
                    #print(v)
                    rows_imported = 0
                    sql = f'select * FROM [dbo].[tb_pokemon_clean]'
                    hook = MsSqlHook(mssql_conn_id="sqlserver")
                    df = hook.get_pandas_df(sql)
                    print(f'importing rows {rows_imported} to {rows_imported + len(df)}... for table {v} ')
                    df.to_gbq( destination_table=f'{dateset_ref}.{table_ref}',  project_id=project_id, credentials=credentials, if_exists="replace" )
                    rows_imported += len(df)
        except Exception as e:
            print("Data load error: " + str(e))
    # call task functions
    tbl_dict = sql_extract()
    tbl_summary = gcp_load(tbl_dict)
#
gcp_extract_and_load = extract_and_load()

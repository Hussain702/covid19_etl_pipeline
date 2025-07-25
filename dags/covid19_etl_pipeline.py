from airflow.decorators import dag,task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.log.logging_mixin import LoggingMixin
from azure.storage.filedatalake import DataLakeServiceClient
from datetime import datetime
import pandas as pd
import requests
import json


API_CONN_ID = 'open_meteo_api'



def upload_to_azure_datalake(file_name,file_path,account_name,account_key,container_name):
    try:
        service_client=DataLakeServiceClient(account_url=f'https://{account_name}.dfs.core.windows.net',credential=account_key)
        file_system_client=service_client.get_file_system_client(file_system=container_name)
        directory_client=file_system_client.get_directory_client('raw')
        try:
            directory_client.create_directory()
        except:
           pass 

        file_client=directory_client.get_file_client(file_name)
        with open(file_path,rb) as f:
            file_contents=f.read()  


        file_client.upload_data(file_contents,overwrite=True)
        print("✅ File uploaded successfully to Azure Data Lake.")
    except Exception as e:
        print("❌ Error uploading to Azure Data Lake:", e)    

    



@dag(
    start_date=datetime.today(),
    schedule='@daily',
    catchup=False
)

def etl_covid_pipeline():

    @task()
    def extract_covid_data():
        http_hook=HttpHook(http_conn_id=API_CONN_ID,method='GET')
        endpoint='v3/covid-19/countries'
        response=http_hook.run(endpoint)
        if response.status_code==200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}") 


    @task()
    def transform_covid_data(covid_data):
        df=pd.DataFrame(covid_data)
        df.drop(['activePerOneMillion','deathsPerOneMillion','criticalPerOneMillion','recoveredPerOneMillion','todayDeaths','critical','todayCases','countryInfo','todayDeaths','todayRecovered'], axis=1, inplace=True)
        return df.to_dict(orient='records') 

    @task()
    def load_covid_data(transformed_data):
        df=pd.DataFrame(transformed_data)
        
        file_path = '/usr/local/airflow/covid.csv'
        df.to_csv('covid.csv',index=False)
        file_name = 'covid.csv'
        account_name = 'freestoragedemo1234'
        account_key = 'Your_actual_key here' #put ur actula key
        container_name = 'mycontainer'

        # Upload
        upload_to_azure_datalake(file_path,file_name, container_name, account_name, account_key)


    covid_data=extract_covid_data()
    transformed_data=transform_covid_data(covid_data)
    load_covid_data(transformed_data)

etl_covid_pipeline()
    
from airflow.decorators import dag, task
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
import json
from pendulum import datetime

# Latitude and longitude for Khemisset, Morocco
LATITUDE = '33.825857'
LONGITUDE = '-6.071467'

API_CONN_ID='open_meteo_api'
POSTGRES_CONN_ID='khemisset_postgres'

@dag(
    dag_id='weather_etl_pipeline',
    schedule="None",
    start_date=datetime(2025, 10, 17),
    catchup = False, 
    description="ETL weather",
    tags = ["ETL", "weather"]
)

def etlweather():

    @task
    def extract_weather_data():

        http_hook=HttpHook(http_conn_id=API_CONN_ID, method='GET')

        endpoint=f'/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'

        ## Make the request via the HTTP Hook
        response=http_hook.run(endpoint)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to fetch weather data: {response.status_code}")

    @task
    def transform_weather_data(weather_data):
        """Transform the extracted weather data."""
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data

    @task
    def load_weather_data(transformed_data):

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        
        with conn.cursor() as cursor:
            # Insert transformed data
            cursor.execute("""
                INSERT INTO weather_data (
                    latitude, longitude, temperature, windspeed, winddirection, weathercode, timestamp
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                transformed_data['latitude'],
                transformed_data['longitude'],
                transformed_data['temperature'],
                transformed_data['windspeed'],
                transformed_data['winddirection'],
                transformed_data['weathercode'],
                transformed_data.get('timestamp')  # will use default CURRENT_TIMESTAMP if not provided
            ))
            
            conn.commit()
    
    
    ## DAG Worflow- ETL Pipeline
    weather_data= extract_weather_data()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data(transformed_data)
    
    
etlweather()
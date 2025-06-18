from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# Ajout du dossier parent au PYTHONPATH pour accéder à pipeline
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import fonctionnel du module
from pipeline.extract.weather_extract import fetch_weather_data

default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False
}

with DAG("weather_dag",
         default_args=default_args,
         schedule_interval="@daily",
         tags=["weather"]) as dag:

    task_extract = PythonOperator(
        task_id="extract_weather",
        python_callable=lambda: fetch_weather_data("Paris", "FR")
    )

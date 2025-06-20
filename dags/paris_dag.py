# dags/weather_history_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajout du chemin pour accéder à pipeline/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import de la fonction depuis le bon fichier
from pipeline.extract.paris_extract import fetch_and_save_history

default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False
}

with DAG("paris_extract",
         default_args=default_args,
         schedule_interval="@once",
         tags=["weather", "history"]) as dag:

    task_fetch_history = PythonOperator(
        task_id="paris-extract",
        python_callable=fetch_and_save_history,
        op_kwargs={
            "start_date": "2023-05-19",
            "output_dir": "/home/unity/airflow/data/paris-23-25/"
        }
    )

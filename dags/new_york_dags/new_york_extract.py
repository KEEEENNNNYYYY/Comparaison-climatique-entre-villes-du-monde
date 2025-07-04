from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajout du chemin pour accéder à pipeline/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction depuis le bon fichier
from pipeline.extract.new_york_extract import fetch_and_save_history

default_args = {
    'start_date': datetime(2020, 1, 1),
    'catchup': False
}

with DAG("new_york_extract",
         default_args=default_args,
         schedule_interval="15 8 * * *", 
         tags=["weather", "history"]) as dag:

    def get_output_path():
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        return os.path.join(base_dir, 'data/data_brut/new_york-20-25')

    task_fetch_history = PythonOperator(
        task_id="new_york_history",
        python_callable=fetch_and_save_history,
        op_kwargs={
            "start_date": "2020-01-01",
            "output_dir": get_output_path()
        }
    )

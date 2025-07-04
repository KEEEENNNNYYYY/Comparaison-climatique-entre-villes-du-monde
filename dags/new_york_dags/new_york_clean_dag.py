from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajouter le chemin du dossier contenant ton script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction de nettoyage de New York
from pipeline.transform.clean.new_york_clean import clean_and_write as clean_new_york_data

default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False
}

with DAG(
    dag_id='new_york_clean_dag',
    schedule_interval="30 8 * * *", 
    default_args=default_args,
    tags=['new_york', 'clean'],
    description='Nettoyage des fichiers JSON bruts de New York',
) as dag:

    clean_task = PythonOperator(
        task_id='clean_new_york_json',
        python_callable=clean_new_york_data
    )

    clean_task

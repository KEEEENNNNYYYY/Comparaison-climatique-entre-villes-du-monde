from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajouter le chemin du dossier contenant ton script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction de nettoyage pour Antananarivo
from pipeline.transform.clean.antananarivo_clean import clean_and_write as clean_antananarivo_data

default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False
}

with DAG(
    dag_id='antananarivo_clean_dag',
    schedule_interval="@once",
    default_args=default_args,
    tags=['antananarivo', 'clean'],
    description='Nettoyage des fichiers JSON bruts d’Antananarivo',
) as dag:

    clean_task = PythonOperator(
        task_id='clean_antananarivo_json',
        python_callable=clean_antananarivo_data
    )

    clean_task

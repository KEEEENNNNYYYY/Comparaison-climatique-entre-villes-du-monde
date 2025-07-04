from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajouter le chemin du dossier contenant ton script
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction ou du script de nettoyage
from pipeline.transform.clean.cape_town_clean  import clean_and_write as clean_cape_town_data


default_args = {
    'start_date': datetime(2024, 6, 1),
    'catchup': False
}

with DAG(
    dag_id='cape_town_clean_dag',
    schedule_interval="30 8 * * *", 
    default_args=default_args,
    tags=['cape_town', 'clean'],
    description='Nettoyage des fichiers JSON bruts de Cape Town',
) as dag:

    clean_task = PythonOperator(
        task_id='clean_cape_town_json',
        python_callable=clean_cape_town_data
    )

    clean_task

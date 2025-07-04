from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajouter le chemin pour que Airflow trouve ton fichier Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction de transformation pour Antananarivo
from pipeline.transform.merge.antananarivo_merge import merge_all_json_to_csv

default_args = {
    'start_date': datetime(2020, 1, 1),
    'catchup': False
}

with DAG(
    dag_id="antananarivo_merge",
    default_args=default_args,
    schedule_interval="45 8 * * *", 
    tags=["weather", "merge", "antananarivo"],
    description="Fusion des fichiers JSON propres d’Antananarivo en un CSV unique"
) as dag:

    task_merge = PythonOperator(
        task_id="merge_antananarivo_data",
        python_callable=merge_all_json_to_csv
    )

    task_merge

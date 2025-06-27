from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajouter le chemin pour que Airflow trouve ton fichier Python
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction de transformation
from pipeline.transform.merge.cape_town_merge.cape_town_weekly import merge_weekly_averages

default_args = {
    'start_date': datetime(2020, 1, 1),
    'catchup': False
}

with DAG("cape_town_merge_weekly",
         default_args=default_args,
         schedule_interval="@once",
         tags=["weather", "weekly", "merge"]

) as dag:
    task_merge_weekly = PythonOperator(
        task_id="merge_weekly_data",
        python_callable=merge_weekly_averages
    )

    task_merge_weekly
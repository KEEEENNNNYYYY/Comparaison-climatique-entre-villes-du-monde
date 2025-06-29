from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction d'upload
from pipeline.load.cape_town_load import upload_csv_to_drive

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG("cape_town_load",
         default_args=default_args,
         schedule_interval="@once",
         tags=["weather", "load"]
) as dag:

    def run_upload():
        # Construire les chemins absolus à partir de la position du script DAG
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        service_account_path = os.path.join(base_dir, 'pipeline/load/service-account.json')
        file_path = os.path.join(base_dir, 'data/data_pret/cape_town.csv')

        upload_csv_to_drive(
            service_account_path=service_account_path,
            file_path=file_path,
            file_name='cape_town.csv',
            folder_id='1RNPT0k2C2ySy8r1XS9g7d-ykFjmHhOuc'
        )

    task_upload = PythonOperator(
        task_id="upload_csv_to_drive",
        python_callable=run_upload
    )

    task_upload

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Ajouter le dossier parent au path pour accéder à `pipeline.load`
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

# Import de la fonction mise à jour
from pipeline.load.new_york_load import upload_csv_to_drive

default_args = {
    'start_date': datetime(2024, 1, 1),
    'catchup': False
}

with DAG(
    "new_york_load",
    default_args=default_args,
    schedule_interval="0 9 * * *", 
    tags=["weather", "load", "new_york"]
) as dag:

    def run_upload():
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '../..'))
        service_account_path = os.path.join(base_dir, 'pipeline/load/service-account.json')
        file_path = os.path.join(base_dir, 'data/data_pret/new_york.csv')

        upload_csv_to_drive(
            service_account_path=service_account_path,
            file_path=file_path,
            file_name='new_york.csv',
            folder_id='1RNPT0k2C2ySy8r1XS9g7d-ykFjmHhOuc' 
        )

    task_upload = PythonOperator(
        task_id="upload_new_york_csv_to_gsheet",
        python_callable=run_upload
    )

    task_upload

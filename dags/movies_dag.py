from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'movies_dag',
    default_args=default_args,
    description='Exemple de DAG utilisant un BashOperator',
    schedule_interval=None,  # désactivé pour un run manuel
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    # Tâche 2: Transformation vers MySQL
    transform_task = BashOperator(
    task_id='preprocess_to_staging',
    bash_command='python /opt/airflow/build/raw_to_staging.py',
    dag=dag,
    )

# Tâche 3: Chargement vers elasticsearch
    load_task = BashOperator(
    task_id='process_to_curated',
    bash_command='python /opt/airflow/build/staging_to_curated.py',
    dag=dag,
    )

   
# Définir l'ordre des tâches
transform_task >> load_task 

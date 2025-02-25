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

    #tâche création de la bucket 
    bucket_creation = BashOperator(
    task_id='bucket_creation',
    bash_command='/opt/airflow/scripts/script.sh '
    )

    # Tâche : Exécuter python /opt/airflow/build/unpack_to_raw.py
    unpack_to_raw_task = BashOperator(
        task_id='unpack_to_raw',
        bash_command='python /opt/airflow/build/unpack_to_raw.py --bucket raw --endpoint http://localstack:4566'
    )

    #tâche création de la bucket 
    mysql_db_creation = BashOperator(
    task_id='mysql_db_creation',
    bash_command='mysql -h mysql -P 3306 -u user -ppassword < /opt/airflow/sql_scripts/init.sql'
    )

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

    #tâche mise en route de l'api 
    start_api = BashOperator(
    task_id='start_api',
    bash_command='uvicorn api:app --host 0.0.0.0 --port 8000 --reload --app-dir api'
    )

# Définir l'ordre des tâches
mysql_db_creation >> bucket_creation >> unpack_to_raw_task >> transform_task >> load_task >> start_api

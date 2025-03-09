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
    'ingest_dag',
    default_args=default_args,
    description='DAG pour ingestion et transformation des données',
    schedule_interval=None,  # désactivé pour un run manuel
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

  
    transform_task = BashOperator(
        task_id='preprocess_to_staging',
        bash_command='python /opt/airflow/scripts_ingest/raw_to_staging_ingest.py',
        dag=dag,
    )

  
transform_task 

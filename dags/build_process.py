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
    'build_process_dag',
    default_args=default_args,
    description='initialisation of the workspace',
    schedule_interval=None,  # désactivé pour un run manuel
    start_date=datetime(2023, 1, 1),
    catchup=False
) as dag:

    #tâche création de la bucket 
    bucket_creation = BashOperator(
    task_id='bucket_creation',
    bash_command='/opt/airflow/scripts/script.sh '
    )

    #tâche création de la bucket 
    mysql_db_creation = BashOperator(
    task_id='mysql_db_creation',
    bash_command='mysql -h mysql -P 3306 -u user -ppassword < /opt/airflow/sql_scripts/init.sql'
    )

     #Tâche : Exécuter python /opt/airflow/build/unpack_to_raw.py
    unpack_to_raw_task = BashOperator(
        task_id='unpack_to_raw',
        bash_command='python /opt/airflow/build/unpack_to_raw.py --bucket raw --endpoint http://localstack:4566'
    )

# Définir l'ordre des tâches
mysql_db_creation >> bucket_creation >> unpack_to_raw_task 